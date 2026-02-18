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
     CollateralAgg AS (SELECT ac.PRFT_ACCOUNT,
                              '[' || LISTAGG(
                                      '{'
                                          || '"collateralPledged":"Cash",'
                                          || '"orgCollateralValue":' || COALESCE(ac.EST_VALUE_AMN, 0) || ','
                                          || '"usdCollateralValue":' ||
                                      CAST(COALESCE(ac.EST_VALUE_AMN, 0) / 2500 AS DECIMAL(15, 2)) || ','
                                          || '"tzsCollateralValue":' || COALESCE(ac.EST_VALUE_AMN, 0)
                                          || '}',
                                      ','
                                     ) || ']' AS collateralPledged
                       FROM ACCOUNT_COLLATERAL ac
                       GROUP BY ac.PRFT_ACCOUNT),
     MainQuery AS (SELECT CURRENT_TIMESTAMP                                                                 AS reportingDate,
                          LTRIM(RTRIM(cust.CUST_ID))                                                        AS customerIdentificationNumber,
                          LTRIM(RTRIM(pa.ACCOUNT_NUMBER))                                                   AS accountNumber,
                          LTRIM(RTRIM(cust.FIRST_NAME))                                                     AS clientName,
                          cl.COUNTRY_CODE                                                                   AS borrowerCountry,
                          'FALSE'                                                                           AS ratingStatus,
                          NULL                                                                              AS crRatingBorrower,
                          'Grade B'                                                                         AS gradesUnratedBanks,
                          lccd.GENDER                                                                       AS gender,
                          NULL                                                                              AS disability,
                          ctl.CUSTOMER_TYPE                                                                 AS clientType,
                          NULL                                                                              AS clientSubType,
                          NULL                                                                              AS groupName,
                          NULL                                                                              AS groupCode,
                          'No relation'                                                                     AS relatedParty,
                          'Direct'                                                                          AS relationshipCategory,
                          pa.ACCOUNT_NUMBER                                                                 AS loanNumber,
                          CASE
                              WHEN p.DESCRIPTION LIKE '%BUSINESS%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                              WHEN p.DESCRIPTION LIKE '%INSURANCE%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                              WHEN p.DESCRIPTION LIKE '%IPF%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                              WHEN p.DESCRIPTION LIKE '%KILIMO%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                              WHEN p.DESCRIPTION LIKE '%MICRO%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                              WHEN p.DESCRIPTION LIKE '%UNSECURED%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                              WHEN p.DESCRIPTION LIKE '%MORTGAGE%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Mortgage Loan'
                              ELSE 'Personal loan'
                              END                                                                           AS loanType,
                          'OtherServices'                                                                   AS loanEconomicActivity,
                          'Existing'                                                                        AS loanPhase,
                          'NotSpecified'                                                                    AS transferStatus,
                          CASE
                              WHEN p.DESCRIPTION LIKE '%MORTGAGE%' AND p.DESCRIPTION LIKE '%LOAN%' THEN
                                  CASE
                                      WHEN GG.DESCRIPTION LIKE '%Development%' THEN 'Improvement'
                                      WHEN GG.DESCRIPTION LIKE '%Purchase%' THEN 'Acquisition'
                                      WHEN GG.DESCRIPTION LIKE '%Construct%' THEN 'Construction'
                                      ELSE 'Others'
                                      END
                              END                                                                           AS purposeMortgage,
                          GG.DESCRIPTION                                                                    AS purposeOtherLoans,
                          'Others'                                                                          AS sourceFundMortgage,
                          'Reducing Method'                                                                 AS amortizationType,
                          la.FK_UNITCODE                                                                    AS branchCode,
                          TRIM(
                                  COALESCE(TRIM(be.FIRST_NAME), '') ||
                                  CASE
                                      WHEN TRIM(be.FATHER_NAME) IS NOT NULL AND TRIM(be.FATHER_NAME) <> ''
                                          THEN ' ' || TRIM(be.FATHER_NAME)
                                      ELSE ''
                                      END ||
                                  CASE
                                      WHEN TRIM(be.LAST_NAME) IS NOT NULL AND TRIM(be.LAST_NAME) <> ''
                                          THEN ' ' || TRIM(be.LAST_NAME)
                                      ELSE ''
                                      END
                          )                                                                                 AS loanOfficer,
                          TRIM(
                                  COALESCE(TRIM(be.FIRST_NAME), '') ||
                                  CASE
                                      WHEN TRIM(be.FATHER_NAME) IS NOT NULL AND TRIM(be.FATHER_NAME) <> ''
                                          THEN ' ' || TRIM(be.FATHER_NAME)
                                      ELSE ''
                                      END ||
                                  CASE
                                      WHEN TRIM(be.LAST_NAME) IS NOT NULL AND TRIM(be.LAST_NAME) <> ''
                                          THEN ' ' || TRIM(be.LAST_NAME)
                                      ELSE ''
                                      END
                          )                                                                                 AS loanSupervisor,

                          NULL                                                                              AS groupVillageNumber,
                          (SELECT COUNT(*)
                           FROM LOAN_ACCOUNT la2
                           WHERE la2.CUST_ID = la.CUST_ID
                             AND la2.ACC_OPEN_DT <= la.ACC_OPEN_DT)                                         AS cycleNumber,
                          la.INSTALL_COUNT                                                                  AS loanInstallment,
                          CASE
                              WHEN la.INSTALL_FREQ = 1 THEN 'Monthly'
                              ELSE 'Monthly'
                              END                                                                           AS repaymentFrequency,
                          cu.SHORT_DESCR                                                                    AS currency,
                          la.ACC_OPEN_DT                                                                    AS contractDate,
                          la.ACC_LIMIT_AMN                                                                  AS orgSanctionedAmount,
                          CASE WHEN cu.SHORT_DESCR = 'USD' THEN la.ACC_LIMIT_AMN ELSE NULL END              AS usdSanctionedAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN la.ACC_LIMIT_AMN * 2500
                              ELSE la.ACC_LIMIT_AMN END                                                     AS tzsSanctionedAmount,
                          la.TOT_DRAWDOWN_AMN                                                               AS orgDisbursedAmount,
                          CASE WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_DRAWDOWN_AMN ELSE NULL END           AS usdDisbursedAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_DRAWDOWN_AMN * 2500
                              ELSE la.TOT_DRAWDOWN_AMN END                                                  AS tzsDisbursedAmount,
                          la.DRAWDOWN_FST_DT                                                                AS disbursementDate,
                          la.ACC_EXP_DT                                                                     AS maturityDate,
                          COALESCE(la.OV_EXP_DT, la.ACC_EXP_DT)                                             AS realEndDate,
                          (la.NRM_CAP_BAL + la.OV_CAP_BAL)                                                  AS orgOutstandingPrincipalAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                              ELSE NULL END                                                                 AS usdOutstandingPrincipalAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 2500
                              ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL) END                                     AS tzsOutstandingPrincipalAmount,
                          li.INSTALL_AMN                                                                    AS orgInstallmentAmount,
                          CASE WHEN cu.SHORT_DESCR = 'USD' THEN li.INSTALL_AMN ELSE NULL END                AS usdInstallmentAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN li.INSTALL_AMN * 2500
                              ELSE li.INSTALL_AMN END                                                       AS tzsInstallmentAmount,
                          la.NRM_INST_CNT                                                                   AS loanInstallmentPaid,
                          NULL                                                                              AS gracePeriodPaymentPrincipal,
                          la.MORATOR_NRM_RATE                                                               AS primeLendingRate,
                          NULL                                                                              AS interestPricingMethod,
                          la.TOT_NRM_INT_AMN                                                                AS annualInterestRate,
                          NULL                                                                              AS effectiveAnnualInterestRate,
                          la.INSTALL_FIRST_DT                                                               AS firstInstallmentPaymentDate,
                          li.RQ_LST_PAYMENT_DT                                                              AS lastPaymentDate,
                          ca.collateralPledged,
                          'Restructured'                                                                    AS loanFlagType,
                          NULL                                                                              AS restructuringDate,
                          li.RQ_OVERDUE_DAYS                                                                AS pastDueDays,
                          la.OV_CAP_BAL                                                                     AS pastDueAmount,
                          la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL                AS orgAccruedInterestAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD'
                                  THEN (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                              ELSE NULL END                                                                 AS usdAccruedInterestAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN
                                  (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * 2500
                              ELSE (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) END AS tzsAccruedInterestAmount,
                          la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL                                      AS orgPenaltyChargedAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN (la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL)
                              ELSE NULL END                                                                 AS usdPenaltyChargedAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN (la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL) * 2500
                              ELSE (la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL) END                       AS tzsPenaltyChargedAmount,
                          COALESCE(la.TOT_PNL_INT_AMN, 0)                                                   AS orgPenaltyPaidAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN COALESCE(la.TOT_PNL_INT_AMN, 0)
                              ELSE NULL END                                                                 AS usdPenaltyPaidAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN COALESCE(la.TOT_PNL_INT_AMN, 0) * 2500
                              ELSE COALESCE(la.TOT_PNL_INT_AMN, 0) END                                      AS tzsPenaltyPaidAmount,
                          la.TOT_COMMISSION_AMN                                                             AS orgLoanFeesChargedAmount,
                          CASE WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_COMMISSION_AMN ELSE NULL END         AS usdLoanFeesChargedAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_COMMISSION_AMN * 2500
                              ELSE la.TOT_COMMISSION_AMN END                                                AS tzsLoanFeesChargedAmount,
                          la.TOT_EXPENSE_AMN                                                                AS orgLoanFeesPaidAmount,
                          CASE WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_EXPENSE_AMN ELSE NULL END            AS usdLoanFeesPaidAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_EXPENSE_AMN * 2500
                              ELSE la.TOT_EXPENSE_AMN END                                                   AS tzsLoanFeesPaidAmount,
                          li.INSTALL_AMN                                                                    AS orgTotMonthlyPaymentAmount,
                          CASE WHEN cu.SHORT_DESCR = 'USD' THEN li.INSTALL_AMN ELSE NULL END                AS usdTotMonthlyPaymentAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN li.INSTALL_AMN * 2500
                              ELSE li.INSTALL_AMN END                                                       AS tzsTotMonthlyPaymentAmount,
                          'Households'                                                                      AS sectorSnaClassification,
                          'Current'                                                                         AS assetClassificationCategory,
                          la.ACC_STATUS                                                                     AS negStatusContract,
                          la.DEP_ACC_TYPE                                                                   AS customerRole,
                          lai.PROVISION_AMOUNT                                                              AS allowanceProbableLoss,
                          lai.PROVISION_AMOUNT                                                              AS botProvision,
                          'Held to Maturity'                                                                AS tradingIntent,
                          la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL                                     AS orgSuspendedInterest,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                              ELSE NULL END                                                                 AS usdSuspendedInterest,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * 2500
                              ELSE (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) END                      AS tzsSuspendedInterest
                   FROM LOAN_ACCOUNT la
                            LEFT JOIN LatestInstallments li ON la.ACC_SN = li.ACC_SN
                            LEFT JOIN ProfitsAccount pa ON pa.CUST_ID = la.CUST_ID
                            LEFT JOIN CollateralAgg ca ON ca.PRFT_ACCOUNT = pa.ACCOUNT_NUMBER
                            LEFT JOIN LOAN_ACCOUNT_INFO lai ON la.FK_UNITCODE = lai.FK_LOAN_ACCOUNTFK
                       AND la.ACC_TYPE = lai.FK0LOAN_ACCOUNTACC
                       AND la.ACC_SN = lai.FK_LOAN_ACCOUNTACC
                            LEFT JOIN CUSTOMER cust ON la.CUST_ID = cust.CUST_ID
                            LEFT JOIN BANKEMPLOYEE be
                                      ON be.STAFF_NO = la.USR
                            LEFT JOIN OtherID id ON id.fk_customercust_id = cust.CUST_ID
                            LEFT JOIN LNS_CRD_CUST_DATA lccd ON lccd.CUST_ID = la.CUST_ID
                            LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = cust.CUST_TYPE
                            LEFT JOIN CURRENCY cu ON cu.ID_CURRENCY = la.FKCUR_IS_CHARGED
                            LEFT JOIN GENERIC_DETAIL id_country ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                       AND id.fkgd_has_been_issu = id_country.serial_num
                            LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
                            LEFT JOIN GENERIC_DETAIL GG ON GG.FK_GENERIC_HEADPAR = la.FKGH_HAS_AS_LOAN_P
                       AND GG.SERIAL_NUM = la.FKGD_HAS_AS_LOAN_P
                            LEFT JOIN PRODUCT p ON la.FK_LOANFK_PRODUCTI = p.ID_PRODUCT)
SELECT *
FROM (SELECT mq.*,
             ROW_NUMBER() OVER (PARTITION BY mq.loanNumber ORDER BY mq.customerIdentificationNumber) AS rn
      FROM MainQuery mq) t
WHERE t.rn = 1
ORDER BY t.customerIdentificationNumber;
