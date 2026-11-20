WITH LatestInstallments AS (SELECT *
                            FROM (SELECT li.*,
                                         ROW_NUMBER() OVER (PARTITION BY li.ACC_SN ORDER BY li.INSTALL_SN DESC) AS rn
                                  FROM LOAN_INSTALLMENTS li) t
                            WHERE t.rn = 1),
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
     MainQuery AS (SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                     AS reportingDate,
                          LTRIM(RTRIM(cust.CUST_ID))                                            AS customerIdentificationNumber,
                          COALESCE(LTRIM(RTRIM(pa.ACCOUNT_NUMBER)), '')                        AS accountNumber,
                          COALESCE(TRIM(
                                  COALESCE(TRIM(cust.FIRST_NAME), '') ||
                                  CASE
                                      WHEN COALESCE(TRIM(cust.MIDDLE_NAME), '') <> ''
                                          THEN ' ' || TRIM(cust.MIDDLE_NAME)
                                      ELSE ''
                                      END ||
                                  CASE
                                      WHEN COALESCE(TRIM(cust.SURNAME), '') <> ''
                                          THEN ' ' || TRIM(cust.SURNAME)
                                      ELSE ''
                                      END
                          ), '')                                                                AS clientName,
                          COALESCE(
                                  CASE WHEN cl.COUNTRY_CODE = 'TZ' THEN 'TANZANIA, UNITED REPUBLIC OF' END,
                                  'TANZANIA, UNITED REPUBLIC OF'
                          )                                                                     AS borrowerCountry,
                          'FALSE'                                                               AS ratingStatus,
                          NULL                                                                  AS crRatingBorrower,
                          'Grade B'                                                             AS gradesUnratedBanks,
                          'Ordinary'                                                            AS borrowerCategory,
                          COALESCE(lccd.GENDER, 'MALE')                                        AS gender,
                          'None'                                                                AS disability,
                          COALESCE(ctl.CUSTOMER_TYPE, 'Unknown')                               AS clientType,
                          NULL                                                                  AS clientSubType,
                          NULL                                                                  AS groupName,
                          NULL                                                                  AS groupCode,
                          'No relation'                                                         AS relatedParty,
                          'Direct'                                                              AS relationshipCategory,
                          pa.ACCOUNT_NUMBER                                                     AS loanNumber,
                          CASE
                              WHEN p.DESCRIPTION LIKE '%BUSINESS%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                              WHEN p.DESCRIPTION LIKE '%INSURANCE%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                              WHEN p.DESCRIPTION LIKE '%IPF%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                              WHEN p.DESCRIPTION LIKE '%KILIMO%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                              WHEN p.DESCRIPTION LIKE '%MICRO%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                              WHEN p.DESCRIPTION LIKE '%UNSECURED%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                              WHEN p.DESCRIPTION LIKE '%MORTGAGE%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Mortgage Loan'
                              ELSE 'Personal loan'
                              END                                                               AS loanType,
                          'OtherServices'                                                       AS loanEconomicActivity,
                          'Existing'                                                            AS loanPhase,
                          'NotSpecified'                                                        AS transferStatus,
                          CASE
                              WHEN p.DESCRIPTION LIKE '%MORTGAGE%' AND p.DESCRIPTION LIKE '%LOAN%' THEN
                                  CASE
                                      WHEN GG.DESCRIPTION LIKE '%Development%' THEN 'Improvement'
                                      WHEN GG.DESCRIPTION LIKE '%Purchase%' THEN 'Acquisition'
                                      WHEN GG.DESCRIPTION LIKE '%Construct%' THEN 'Construction'
                                      ELSE 'Others'
                                      END
                              END                                                               AS purposeMortgage,

                          CASE WHEN GG.DESCRIPTION ='Development' THEN GG.DESCRIPTION ELSE 'Other' END AS purposeOtherLoans,
                          'Others'                                                              AS sourceFundMortgage,
                          'Reducing Method'                                                     AS amortizationType,
                          COALESCE(la.FK_UNITCODE, '')                                          AS branchCode,
                          COALESCE(TRIM(
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
                          ), '')                                                                AS loanOfficer,
                          COALESCE(TRIM(
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
                          ), '')                                                                AS loanSupervisor,
                          NULL                                                                  AS groupVillageNumber,
                          (SELECT COUNT(*)
                           FROM LOAN_ACCOUNT la2
                           WHERE la2.CUST_ID = la.CUST_ID
                             AND la2.ACC_OPEN_DT <= la.ACC_OPEN_DT)                             AS cycleNumber,
                          COALESCE(la.INSTALL_COUNT, 0)                                         AS loanInstallment,
                          CASE
                              WHEN la.INSTALL_FREQ = 1 THEN '30 days'
                              ELSE 'Irregular'
                              END                                                               AS repaymentFrequency,
                          COALESCE(cu.SHORT_DESCR, 'TZS')                                       AS currency,
                          COALESCE(
                              VARCHAR_FORMAT(
                                  NULLIF(DATE(la.ACC_OPEN_DT), DATE('0001-01-01')),
                                  'DDMMYYYYHHMM'
                              ),
                              ''
                          )                                                                     AS contractDate,
                          COALESCE(la.ACC_LIMIT_AMN, 0)                                         AS orgSanctionAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'TZS' THEN 0
                              WHEN cu.SHORT_DESCR = 'USD' THEN la.ACC_LIMIT_AMN
                              WHEN cu.SHORT_DESCR <> 'USD'
                                  THEN DECIMAL(la.ACC_LIMIT_AMN / fx.rate, 18, 2)
                              ELSE 0
                              END                                                               AS usdSanctionAmount,
                          COALESCE(CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN DECIMAL(la.ACC_LIMIT_AMN * fx.RATE, 18, 2)
                              ELSE la.ACC_LIMIT_AMN
                              END, 0)                                                           AS tzsSanctionAmount,
                          COALESCE(la.TOT_DRAWDOWN_AMN, 0)                                      AS orgDisbursedAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'TZS' THEN 0
                              WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_DRAWDOWN_AMN
                              WHEN cu.SHORT_DESCR <> 'USD' THEN DECIMAL(la.ACC_LIMIT_AMN / fx.RATE, 18, 2)
                              ELSE 0
                              END                                                               AS usdDisbursedAmount,
                          COALESCE(CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN DECIMAL(la.TOT_DRAWDOWN_AMN * fx.RATE, 18, 2)
                              ELSE la.TOT_DRAWDOWN_AMN
                              END, 0)                                                           AS tzsDisbursedAmount,
                          COALESCE(
                              VARCHAR_FORMAT(
                                  COALESCE(
                                      NULLIF(DATE(la.DRAWDOWN_FST_DT), DATE('0001-01-01')),
                                      NULLIF(DATE(la.ACC_OPEN_DT), DATE('0001-01-01'))
                                  ),
                                  'DDMMYYYYHHMM'
                              ),
                              ''
                          )                                                                     AS disbursementDate,
                          COALESCE(
                              VARCHAR_FORMAT(
                                  NULLIF(DATE(la.ACC_EXP_DT), DATE('0001-01-01')),
                                  'DDMMYYYYHHMM'
                              ),
                              ''
                          )                                                                     AS maturityDate,
                          COALESCE(
                              VARCHAR_FORMAT(
                                  COALESCE(
                                      NULLIF(DATE(la.OV_EXP_DT), DATE('0001-01-01')),
                                      NULLIF(DATE(la.ACC_EXP_DT), DATE('0001-01-01'))
                                  ),
                                  'DDMMYYYYHHMM'
                              ),
                              ''
                          )                                                                     AS realEndDate,
                          (COALESCE(la.NRM_CAP_BAL, 0) + COALESCE(la.OV_CAP_BAL, 0))           AS orgOutstandingPrincipalAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'TZS' THEN 0
                              WHEN cu.SHORT_DESCR = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                              WHEN cu.SHORT_DESCR <> 'USD'
                                  THEN DECIMAL((la.NRM_CAP_BAL + la.OV_CAP_BAL) / fx.rate, 18, 2)
                              ELSE NULL
                              END                                                               AS usdOutstandingPrincipalAmount,
                          COALESCE(CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN (COALESCE(la.NRM_CAP_BAL, 0) + COALESCE(la.OV_CAP_BAL, 0)) * fx.RATE
                              ELSE (COALESCE(la.NRM_CAP_BAL, 0) + COALESCE(la.OV_CAP_BAL, 0))
                              END, 0)                                                           AS tzsOutstandingPrincipalAmount,
                          COALESCE(li.INSTALL_AMN, 0)                                           AS orgInstallmentAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'TZS' THEN 0
                              WHEN cu.SHORT_DESCR = 'USD' THEN li.INSTALL_AMN
                              ELSE NULL
                              END                                                               AS usdInstallmentAmount,
                          COALESCE(CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN DECIMAL(li.INSTALL_AMN * fx.RATE, 18, 2)
                              ELSE li.INSTALL_AMN
                              END, 0)                                                           AS tzsInstallmentAmount,
                          COALESCE(la.NRM_INST_CNT, 0)                                          AS loanInstallmentPaid,
                          NULL                                                                  AS gracePeriodPaymentPrincipal,
                          COALESCE(DECIMAL(la.MORATOR_NRM_RATE, 18, 2), 0)                      AS primeLendingRate,
                          'Fixed'                                                               AS interestPricingMethod,
                          COALESCE(la.TOT_NRM_INT_AMN, 0)                                       AS annualInterestRate,
                          NULL                                                                  AS effectiveAnnualInterestRate,
                          COALESCE(
                              VARCHAR_FORMAT(
                                  COALESCE(
                                      NULLIF(DATE(la.INSTALL_FIRST_DT), DATE('0001-01-01')),
                                      NULLIF(DATE(la.ACC_OPEN_DT), DATE('0001-01-01'))
                                  ),
                                  'DDMMYYYYHHMM'
                              ),
                              ''
                          )                                                                     AS firstInstallmentPaymentDate,
                          COALESCE(
                              VARCHAR_FORMAT(
                                  COALESCE(
                                      NULLIF(DATE(li.RQ_LST_PAYMENT_DT), DATE('0001-01-01')),
                                      NULLIF(DATE(li.REQUEST_DT), DATE('0001-01-01')),
                                      NULLIF(DATE(la.DRAWDOWN_FST_DT), DATE('0001-01-01')),
                                      NULLIF(DATE(la.ACC_OPEN_DT), DATE('0001-01-01'))
                                  ),
                                  'DDMMYYYYHHMM'
                              ),
                              ''
                          )                                                                     AS lastPaymentDate,
                          ca.collateralPledged,
                          CASE
                              WHEN la.OV_EXP_DT IS NOT NULL THEN 'Restructured'
                              ELSE 'NotRestructured'
                              END                                                               AS loanFlagType,
                          CASE
                              WHEN NULLIF(DATE(la.OV_EXP_DT), DATE('0001-01-01')) IS NOT NULL
                                  THEN VARCHAR_FORMAT(
                                      NULLIF(DATE(la.OV_EXP_DT), DATE('0001-01-01')),
                                      'DDMMYYYYHHMM'
                                  )
                              ELSE NULL
                              END                                                               AS restructuringDate,
                          COALESCE(li.RQ_OVERDUE_DAYS, 0)                                       AS pastDueDays,
                          COALESCE(la.OV_CAP_BAL, 0)                                            AS pastDueAmount,
                          'Unclassified'                                                        AS internalRiskGroup,
                          COALESCE(la.NRM_ACR_INT_BAL, 0) + COALESCE(la.OV_ACR_NRM_INT_BAL, 0) + COALESCE(la.OV_ACR_PNL_INT_BAL, 0) AS orgAccruedInterestAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'TZS' THEN 0
                              WHEN cu.SHORT_DESCR = 'USD'
                                  THEN (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                              WHEN cu.SHORT_DESCR <> 'USD'
                                  THEN (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) / fx.RATE
                              ELSE 0
                              END                                                               AS usdAccruedInterestAmount,
                          COALESCE(CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN
                                  DECIMAL((COALESCE(la.NRM_ACR_INT_BAL, 0) + COALESCE(la.OV_ACR_NRM_INT_BAL, 0) + COALESCE(la.OV_ACR_PNL_INT_BAL, 0)) *
                                          fx.RATE, 18, 2)
                              ELSE (COALESCE(la.NRM_ACR_INT_BAL, 0) + COALESCE(la.OV_ACR_NRM_INT_BAL, 0) + COALESCE(la.OV_ACR_PNL_INT_BAL, 0))
                              END, 0)                                                           AS tzsAccruedInterestAmount,
                          COALESCE(la.OV_RL_PNL_INT_BAL, 0) + COALESCE(la.OV_URL_PNL_INT_BAL, 0) AS orgPenaltyChargedAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'TZS' THEN 0
                              WHEN cu.SHORT_DESCR = 'USD' THEN (la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL)
                              WHEN cu.SHORT_DESCR <> 'USD'
                                  THEN DECIMAL((la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL) / fx.RATE, 18, 2)
                              ELSE 0
                              END                                                               AS usdPenaltyChargedAmount,
                          COALESCE(CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN
                                  DECIMAL((COALESCE(la.OV_RL_PNL_INT_BAL, 0) + COALESCE(la.OV_URL_PNL_INT_BAL, 0)) * fx.RATE, 18, 2)
                              ELSE (COALESCE(la.OV_RL_PNL_INT_BAL, 0) + COALESCE(la.OV_URL_PNL_INT_BAL, 0))
                              END, 0)                                                           AS tzsPenaltyChargedAmount,
                          COALESCE(la.TOT_PNL_INT_AMN, 0)                                      AS orgPenaltyPaidAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'TZS' THEN 0
                              WHEN cu.SHORT_DESCR = 'USD' THEN COALESCE(la.TOT_PNL_INT_AMN, 0)
                              WHEN cu.SHORT_DESCR <> 'USD'
                                  THEN DECIMAL(la.TOT_PNL_INT_AMN / fx.rate, 18, 2)
                              ELSE 0
                              END                                                               AS usdPenaltyPaidAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN
                                  DECIMAL(COALESCE(la.TOT_PNL_INT_AMN, 0) * fx.RATE, 18, 2)
                              ELSE COALESCE(la.TOT_PNL_INT_AMN, 0)
                              END                                                               AS tzsPenaltyPaidAmount,
                          COALESCE(la.TOT_COMMISSION_AMN, 0)                                    AS orgLoanFeesChargedAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'TZS' THEN 0
                              WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_COMMISSION_AMN
                              WHEN cu.SHORT_DESCR <> 'USD'
                                  THEN DECIMAL(la.TOT_COMMISSION_AMN / fx.RATE, 18, 2)
                              ELSE 0
                              END                                                               AS usdLoanFeesChargedAmount,
                          COALESCE(CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN DECIMAL(la.TOT_COMMISSION_AMN * fx.RATE, 18, 2)
                              ELSE la.TOT_COMMISSION_AMN
                              END, 0)                                                           AS tzsLoanFeesChargedAmount,
                          COALESCE(la.TOT_EXPENSE_AMN, 0)                                       AS orgLoanFeesPaidAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'TZS' THEN 0
                              WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_EXPENSE_AMN
                              WHEN cu.SHORT_DESCR <> 'USD' THEN DECIMAL(la.TOT_EXPENSE_AMN / fx.RATE, 18, 2)
                              ELSE 0
                              END                                                               AS usdLoanFeesPaidAmount,
                          COALESCE(CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN DECIMAL(la.TOT_EXPENSE_AMN * fx.RATE, 18, 2)
                              ELSE la.TOT_EXPENSE_AMN
                              END, 0)                                                           AS tzsLoanFeesPaidAmount,
                          COALESCE(li.INSTALL_AMN, 0)                                           AS orgTotMonthlyPaymentAmount,
                          CASE
                              WHEN cu.SHORT_DESCR = 'TZS' THEN 0
                              WHEN cu.SHORT_DESCR = 'USD' THEN li.INSTALL_AMN
                              WHEN cu.SHORT_DESCR <> 'USD' THEN DECIMAL(li.INSTALL_AMN / fx.RATE, 18, 2)
                              ELSE 0
                              END                                                               AS usdTotMonthlyPaymentAmount,
                          COALESCE(CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN DECIMAL(li.INSTALL_AMN * fx.rate, 18, 2)
                              ELSE DECIMAL(li.INSTALL_AMN, 18, 2)
                              END, 0)                                                           AS tzsTotMonthlyPaymentAmount,
                          'Households'                                                          AS sectorSnaClassification,
                          'Current'                                                             AS assetClassificationCategory,
                          COALESCE(la.ACC_STATUS, 'Unknown')                                    AS negStatusContract,
                          'Personal Loan'                                  AS customerRole,
                          COALESCE(lai.PROVISION_AMOUNT, 0)                                     AS allowanceProbableLoss,
                          COALESCE(lai.PROVISION_AMOUNT, 0)                                     AS botProvision,
                          'Held to Maturity'                                                    AS tradingIntent,
                          COALESCE(la.OV_ACR_NRM_INT_BAL, 0) + COALESCE(la.OV_ACR_PNL_INT_BAL, 0) AS orgSuspendedInterest,
                          CASE
                              WHEN cu.SHORT_DESCR = 'TZS' THEN 0
                              WHEN cu.SHORT_DESCR = 'USD' THEN (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                              WHEN cu.SHORT_DESCR <> 'USD'
                                  THEN DECIMAL((la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) / fx.RATE, 18, 2)
                              ELSE 0
                              END                                                               AS usdSuspendedInterest,
                          COALESCE(CASE
                              WHEN cu.SHORT_DESCR = 'USD' THEN
                                  DECIMAL((COALESCE(la.OV_ACR_NRM_INT_BAL, 0) + COALESCE(la.OV_ACR_PNL_INT_BAL, 0)) * fx.RATE, 18, 2)
                              ELSE (COALESCE(la.OV_ACR_NRM_INT_BAL, 0) + COALESCE(la.OV_ACR_PNL_INT_BAL, 0))
                              END, 0)                                                           AS tzsSuspendedInterest,
                          la.TMSTAMP                                                            AS tmstamp

                   FROM LOAN_ACCOUNT la

                            LEFT JOIN LatestInstallments li
                                ON la.ACC_SN = li.ACC_SN

                            LEFT JOIN (
                                SELECT *
                                FROM (
                                    SELECT pa.*,
                                           ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER, PRFT_SYSTEM) AS rn
                                    FROM PROFITS_ACCOUNT pa
                                    WHERE PRFT_SYSTEM = 4
                                )
                                WHERE rn = 1
                            ) pa ON pa.CUST_ID = la.CUST_ID

                            LEFT JOIN CollateralAgg ca
                                ON ca.PRFT_ACCOUNT = pa.ACCOUNT_NUMBER

                            LEFT JOIN LOAN_ACCOUNT_INFO lai
                                ON la.FK_UNITCODE = lai.FK_LOAN_ACCOUNTFK
                               AND la.ACC_TYPE    = lai.FK0LOAN_ACCOUNTACC
                               AND la.ACC_SN      = lai.FK_LOAN_ACCOUNTACC

                            LEFT JOIN CUSTOMER cust
                                ON la.CUST_ID = cust.CUST_ID

                            LEFT JOIN CURRENCY curr
                                ON curr.ID_CURRENCY = la.FKCUR_USES_AS_LIM

                            LEFT JOIN (
                                SELECT fr.fk_currencyid_curr, fr.rate
                                FROM fixing_rate fr
                                WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN (
                                    SELECT fk_currencyid_curr,
                                           activation_date,
                                           MAX(activation_time)
                                    FROM fixing_rate
                                    WHERE activation_date = (
                                        SELECT MAX(b.activation_date)
                                        FROM fixing_rate b
                                        WHERE b.activation_date <= CURRENT_DATE
                                    )
                                    GROUP BY fk_currencyid_curr, activation_date
                                )
                            ) fx ON fx.fk_currencyid_curr = curr.ID_CURRENCY

                            LEFT JOIN BANKEMPLOYEE be
                                ON be.STAFF_NO = la.USR

                            LEFT JOIN OtherID id
                                ON id.fk_customercust_id = cust.CUST_ID

                            LEFT JOIN LNS_CRD_CUST_DATA lccd
                                ON lccd.CUST_ID = la.CUST_ID

                            LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl
                                ON ctl.CUSTOMER_TYPE_CODE = cust.CUST_TYPE

                            LEFT JOIN CURRENCY cu
                                ON cu.ID_CURRENCY = la.FKCUR_IS_CHARGED

                            LEFT JOIN GENERIC_DETAIL id_country
                                ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                               AND id.fkgd_has_been_issu = id_country.serial_num

                            LEFT JOIN COUNTRIES_LOOKUP cl
                                ON cl.COUNTRY_NAME = id_country.description

                            LEFT JOIN GENERIC_DETAIL GG
                                ON GG.FK_GENERIC_HEADPAR = la.FKGH_HAS_AS_LOAN_P
                               AND GG.SERIAL_NUM = la.FKGD_HAS_AS_LOAN_P

                            LEFT JOIN PRODUCT p
                                ON la.FK_LOANFK_PRODUCTI = p.ID_PRODUCT
                            {timestamp_filter})

SELECT *
FROM (
    SELECT mq.*,
           ROW_NUMBER() OVER (
               PARTITION BY mq.loanNumber
               ORDER BY mq.customerIdentificationNumber, mq.accountNumber
           ) AS rn
    FROM MainQuery mq
) t
WHERE t.rn = 1
  AND NULLIF(TRIM(t.loanOfficer), '') IS NOT NULL
  AND NULLIF(TRIM(t.loanSupervisor), '') IS NOT NULL
  AND LENGTH(TRIM(TRANSLATE(t.loanOfficer, '', '0123456789'))) = LENGTH(TRIM(t.loanOfficer))
  AND LENGTH(TRIM(TRANSLATE(t.loanSupervisor, '', '0123456789'))) = LENGTH(TRIM(t.loanSupervisor))
ORDER BY t.customerIdentificationNumber;
