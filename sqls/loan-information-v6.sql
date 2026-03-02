WITH LatestInstallments AS (SELECT *
                            FROM (SELECT li.*,
                                         ROW_NUMBER() OVER (PARTITION BY li.ACC_SN ORDER BY li.INSTALL_SN DESC) AS rn
                                  FROM LOAN_INSTALLMENTS li) t
                            WHERE t.rn = 1),
     ProfitsAccount AS (SELECT CUST_ID,
                               MIN(ACCOUNT_NUMBER) AS ACCOUNT_NUMBER
                        FROM PROFITS_ACCOUNT
                        GROUP BY CUST_ID),
     OtherID AS (SELECT fk_customercust_id,
                        MAX(ID_NO)              AS ID_NO,
                        MAX(fkgh_has_been_issu) AS fkgh_has_been_issu,
                        MAX(fkgd_has_been_issu) AS fkgd_has_been_issu
                 FROM other_id
                 WHERE COALESCE(main_flag, '1') = '1'
                 GROUP BY fk_customercust_id),
     CollateralAgg AS (SELECT COLLTBL_CUST_ID,
                              '[' || LISTAGG(
                                      '{'
                                          || '"collateralPledged":"' || CASE
                                                                            WHEN FK_COLLATERAL_TFK = 20030
                                                                                THEN 'MotorVehicles'
                                                                            ELSE 'Cash'
                                          END || '",'
                                          || '"orgCollateralValue":' || COALESCE(TOT_EST_VALUE_AMN, 0) || ','
                                          || '"usdCollateralValue":' ||
                                      CAST(COALESCE(TOT_EST_VALUE_AMN, 0) / 2500 AS DECIMAL(15, 2)) || ','
                                          || '"tzsCollateralValue":' || COALESCE(TOT_EST_VALUE_AMN, 0)
                                          || '}',
                                      ','
                                     ) || ']' AS collateralPledged
                       FROM COLLATERAL
                       GROUP BY COLLTBL_CUST_ID),
     MainQuery AS (SELECT *
                   FROM (SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                            AS reportingDate,
                                LTRIM(RTRIM(cust.CUST_ID))                                                   AS customerIdentificationNumber,
                                LTRIM(RTRIM(pa.ACCOUNT_NUMBER))                                              AS accountNumber,
                                (
                                    TRIM(BOTH FROM COALESCE(cust.FIRST_NAME, '')) ||
                                    CASE
                                        WHEN COALESCE(cust.MIDDLE_NAME, '') <> ''
                                            THEN ' ' || TRIM(BOTH FROM cust.MIDDLE_NAME)
                                        ELSE ''
                                        END ||
                                    CASE
                                        WHEN COALESCE(cust.SURNAME, '') <> '' THEN ' ' || TRIM(BOTH FROM cust.SURNAME)
                                        ELSE ''
                                        END
                                    )                                                                        AS clientName,
                                'TANZANIA, UNITED REPUBLIC OF'                                               AS borrowerCountry,
                                'FALSE'                                                                      AS ratingStatus,
                                NULL                                                                         AS crRatingBorrower,
                                'Grade B'                                                                    AS gradesUnratedBanks,
                                'Ordinary'                                                                   AS borrowerCategory,
                                COALESCE(lccd.GENDER, 'MALE')                                                AS gender,
                                'None'                                                                       AS disability,
                                ctl.CUSTOMER_TYPE                                                            AS clientType,
                                NULL                                                                         AS clientSubType,
                                NULL                                                                         AS groupName,
                                NULL                                                                         AS groupCode,
                                'No relation'                                                                AS relatedParty,
                                'Direct'                                                                     AS relationshipCategory,
                                pa.ACCOUNT_NUMBER                                                            AS loanNumber,
                                CASE
                                    WHEN p.DESCRIPTION LIKE '%MORTGAGE%' AND p.DESCRIPTION LIKE '%LOAN%'
                                        THEN 'Mortgage Loan'
                                    ELSE 'Business Loan'
                                    END                                                                      AS loanType,
                                'OtherServices'                                                              AS loanEconomicActivity,
                                'Existing'                                                                   AS loanPhase,
                                'NotSpecified'                                                               AS transferStatus,
                                CASE
                                    WHEN p.DESCRIPTION LIKE '%MORTGAGE%' AND p.DESCRIPTION LIKE '%LOAN%' THEN
                                        CASE
                                            WHEN GG.DESCRIPTION LIKE '%Development%' THEN 'Improvement'
                                            WHEN GG.DESCRIPTION LIKE '%Purchase%' THEN 'Acquisition'
                                            WHEN GG.DESCRIPTION LIKE '%Construct%' THEN 'Construction'
                                            ELSE 'Others'
                                            END
                                    END                                                                      AS purposeMortgage,
                                GG.DESCRIPTION                                                               AS purposeOtherLoans,
                                'Others'                                                                     AS sourceFundMortgage,
                                'Reducing Method'                                                            AS amortizationType,
                                la.FK_UNITCODE                                                               AS branchCode,
                                TRIM(
                                        COALESCE(TRIM(be.FIRST_NAME), '') ||
                                        CASE
                                            WHEN TRIM(be.FATHER_NAME) IS NOT NULL AND TRIM(be.FATHER_NAME) <> ''
                                                THEN ' ' || TRIM(be.FATHER_NAME)
                                            ELSE '' END ||
                                        CASE
                                            WHEN TRIM(be.LAST_NAME) IS NOT NULL AND TRIM(be.LAST_NAME) <> ''
                                                THEN ' ' || TRIM(be.LAST_NAME)
                                            ELSE '' END
                                )                                                                            AS loanOfficer,
                                TRIM(
                                        COALESCE(TRIM(be.FIRST_NAME), '') ||
                                        CASE
                                            WHEN TRIM(be.FATHER_NAME) IS NOT NULL AND TRIM(be.FATHER_NAME) <> ''
                                                THEN ' ' || TRIM(be.FATHER_NAME)
                                            ELSE '' END ||
                                        CASE
                                            WHEN TRIM(be.LAST_NAME) IS NOT NULL AND TRIM(be.LAST_NAME) <> ''
                                                THEN ' ' || TRIM(be.LAST_NAME)
                                            ELSE '' END
                                )                                                                            AS loanSupervisor,
                                NULL                                                                         AS groupVillageNumber,
                                (SELECT COUNT(*)
                                 FROM LOAN_ACCOUNT la2
                                 WHERE la2.CUST_ID = la.CUST_ID
                                   AND la2.ACC_OPEN_DT <= la.ACC_OPEN_DT)                                    AS cycleNumber,
                                la.INSTALL_COUNT                                                             AS loanInstallment,
                                CASE WHEN la.INSTALL_FREQ = 1 THEN '30 days' ELSE 'Irregular' END            AS repaymentFrequency,
                                cu.SHORT_DESCR                                                               AS currency,
                                VARCHAR_FORMAT(la.ACC_OPEN_DT, 'DDMMYYYYHHMM')                               AS contractDate,
                                la.ACC_LIMIT_AMN                                                             AS orgSanctionedAmount,
                                CASE
                                    WHEN cu.SHORT_DESCR = 'USD' THEN la.ACC_LIMIT_AMN
                                    ELSE DECIMAL(la.ACC_LIMIT_AMN / fx.rate, 18, 2) END                      AS usdSanctionedAmount,
                                CASE
                                    WHEN cu.SHORT_DESCR = 'USD' THEN DECIMAL(la.ACC_LIMIT_AMN * fx.RATE, 18, 2)
                                    ELSE la.ACC_LIMIT_AMN END                                                AS tzsSanctionedAmount,
                                la.TOT_DRAWDOWN_AMN                                                          AS orgDisbursedAmount,
                                CASE
                                    WHEN cu.SHORT_DESCR = 'USD' THEN COALESCE(DECIMAL(la.TOT_DRAWDOWN_AMN, 18, 2), 0)
                                    ELSE COALESCE(DECIMAL(la.ACC_LIMIT_AMN / fx.RATE, 18, 2), 0) END         AS usdDisbursedAmount,
                                CASE
                                    WHEN cu.SHORT_DESCR = 'USD' THEN DECIMAL(la.TOT_DRAWDOWN_AMN * fx.RATE, 18, 2)
                                    ELSE DECIMAL(la.TOT_DRAWDOWN_AMN, 18, 2) END                             AS tzsDisbursedAmount,
                                CollateralAgg.collateralPledged                                              AS collateralPledged,
                                la.NRM_CAP_BAL + la.OV_CAP_BAL                                               AS orgOutstandingPrincipalAmount,
                                CASE
                                    WHEN cu.SHORT_DESCR = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                                    ELSE DECIMAL((la.NRM_CAP_BAL + la.OV_CAP_BAL) / fx.rate, 18, 2) END      AS usdOutstandingPrincipalAmount,
                                CASE
                                    WHEN cu.SHORT_DESCR = 'USD'
                                        THEN DECIMAL((la.NRM_CAP_BAL + la.OV_CAP_BAL) * fx.RATE, 18, 2)
                                    ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL) END                                AS tzsOutstandingPrincipalAmount,
                                COALESCE(li.INSTALL_AMN, 0)                                                  AS orgInstallmentAmount,
                                CASE WHEN cu.SHORT_DESCR = 'USD' THEN li.INSTALL_AMN ELSE NULL END           AS usdInstallmentAmount,
                                CASE
                                    WHEN cu.SHORT_DESCR = 'USD'
                                        THEN DECIMAL(COALESCE(li.INSTALL_AMN, 0) * fx.RATE, 18, 2)
                                    ELSE COALESCE(li.INSTALL_AMN, 0) END                                     AS tzsInstallmentAmount,
                                la.NRM_INST_CNT                                                              AS loanInstallmentPaid,
                                lai.PROVISION_AMOUNT                                                         AS allowanceProbableLoss,
                                lai.PROVISION_AMOUNT                                                         AS botProvision,
                                'Held to Maturity'                                                           AS tradingIntent,
                                la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL                                AS orgSuspendedInterest,
                                CASE
                                    WHEN cu.SHORT_DESCR = 'USD' THEN (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                                    ELSE DECIMAL((la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) / fx.RATE, 18,
                                                 2) END                                                      AS usdSuspendedInterest,
                                CASE
                                    WHEN cu.SHORT_DESCR = 'USD' THEN DECIMAL(
                                            (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * fx.RATE, 18, 2)
                                    ELSE DECIMAL((la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL), 18, 2) END AS tzsSuspendedInterest
                         FROM LOAN_ACCOUNT la
                                  LEFT JOIN LatestInstallments li ON la.ACC_SN = li.ACC_SN
                                  LEFT JOIN ProfitsAccount pa ON pa.CUST_ID = la.CUST_ID
                                  LEFT JOIN CollateralAgg ON CollateralAgg.COLLTBL_CUST_ID = la.CUST_ID
                                  LEFT JOIN LOAN_ACCOUNT_INFO lai ON la.FK_UNITCODE = lai.FK_LOAN_ACCOUNTFK
                             AND la.ACC_TYPE = lai.FK0LOAN_ACCOUNTACC
                             AND la.ACC_SN = lai.FK_LOAN_ACCOUNTACC
                                  LEFT JOIN CUSTOMER cust ON la.CUST_ID = cust.CUST_ID
                                  LEFT JOIN CURRENCY curr ON curr.ID_CURRENCY = la.FKCUR_USES_AS_LIM
                                  LEFT JOIN (SELECT fr.fk_currencyid_curr, fr.rate
                                             FROM fixing_rate fr
                                             WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN
                                                   (SELECT fk_currencyid_curr, activation_date, MAX(activation_time)
                                                    FROM fixing_rate
                                                    WHERE activation_date = (SELECT MAX(b.activation_date)
                                                                             FROM fixing_rate b
                                                                             WHERE b.activation_date <= CURRENT_DATE)
                                                    GROUP BY fk_currencyid_curr, activation_date)) fx
                                            ON fx.fk_currencyid_curr = curr.ID_CURRENCY
                                  LEFT JOIN BANKEMPLOYEE be ON be.STAFF_NO = la.USR
                                  LEFT JOIN OtherID id ON id.fk_customercust_id = cust.CUST_ID
                                  LEFT JOIN LNS_CRD_CUST_DATA lccd ON lccd.CUST_ID = la.CUST_ID
                                  LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = cust.CUST_TYPE
                                  LEFT JOIN CURRENCY cu ON cu.ID_CURRENCY = la.FKCUR_IS_CHARGED
                                  LEFT JOIN GENERIC_DETAIL id_country
                                            ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                                                AND id.fkgd_has_been_issu = id_country.serial_num
                                  LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
                                  LEFT JOIN GENERIC_DETAIL GG ON GG.FK_GENERIC_HEADPAR = la.FKGH_HAS_AS_LOAN_P
                             AND GG.SERIAL_NUM = la.FKGD_HAS_AS_LOAN_P
                                  LEFT JOIN PRODUCT p ON la.FK_LOANFK_PRODUCTI = p.ID_PRODUCT))
SELECT *
FROM (SELECT mq.*,
             ROW_NUMBER() OVER (PARTITION BY mq.loanNumber ORDER BY mq.customerIdentificationNumber) AS rn
      FROM MainQuery mq) t
WHERE t.rn = 1
  AND LENGTH(TRIM(TRANSLATE(t.loanOfficer, '', '0123456789'))) = LENGTH(TRIM(t.loanOfficer))
   AND LENGTH(TRIM(TRANSLATE(t.loanSupervisor, '', '0123456789'))) = LENGTH(TRIM(t.loanSupervisor))
ORDER BY t.customerIdentificationNumber;