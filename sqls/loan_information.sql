select CURRENT_TIMESTAMP                                                        AS reportingDate,
       LTRIM(RTRIM(id.ID_NO))                                                   AS customerIdentificationNumber,
       LTRIM(RTRIM(pa.ACCOUNT_NUMBER))                                          AS accountNumber,
       LTRIM(RTRIM(wela.CUSTOMER_NAME))                                         AS clientName,
       cl.COUNTRY_CODE                                                          as borrowerCountry,
       null                                                                     as ratingStatus,
       null                                                                     as crRatingBorrower,
       null                                                                     as gradesUnratedBanks,
       lccd.GENDER                                                              as gender,
       lccd.GENDER                                                              as disability,
       ctl.CUSTOMER_TYPE                                                        as clientType,
       null                                                                     as clientSubType,
       null                                                                     as groupName,
       null                                                                     as groupCode,
       'No relation'                                                            as relatedParty,
       'Direct'                                                                 as relationshipCategory,
       wela.ACCOUNT_NUMBER                                                      as loanNumber,
       CASE
           WHEN PRODUCT_DESC LIKE '%PERSONAL%' AND PRODUCT_DESC LIKE '%LOAN%'
               THEN 'Personal Loan'

           WHEN PRODUCT_DESC LIKE '%BUSINESS%' AND PRODUCT_DESC LIKE '%LOAN%'
               THEN 'Business Loan'

           WHEN PRODUCT_DESC LIKE '%MORTAGE%' AND PRODUCT_DESC LIKE '%LOAN%'
               THEN 'Mortage Loan'

           ELSE 'Unknown'
           END                                                                  AS loanType,
       'OtherServices'                                                          as loanEconomicActivity,
       'Existing'                                                               as loanPhase,
       'NotSpecified'                                                           as transferStatus,

       CASE
           WHEN wela.PRODUCT_DESC LIKE '%MORTGAGE%' AND wela.PRODUCT_DESC LIKE '%LOAN%'
               THEN
               CASE
                   WHEN GG.DESCRIPTION LIKE '%Development%' THEN 'Improvement'
                   WHEN GG.DESCRIPTION LIKE '%Purchase%' THEN 'Acquisition'
                   WHEN GG.DESCRIPTION LIKE '%Construct%' THEN 'Construction'
                   WHEN GG.DESCRIPTION LIKE '%Others%' THEN 'Others'
                   ELSE 'Unknown'
                   END
           END                                                                  AS purposeMortgage,
       GG.DESCRIPTION                                                           as purposeOtherLoans,
       'Others'                                                                 as sourceFundMortgage,
       'Others'                                                                 as sourceFundMortgage,
       'Reducing Method'                                                        as amortizationType,
       wela.FK_UNITCODE                                                         as branchCode,
       wela.LOAN_OFFICER_NAME                                                   as loanOfficer,
       null                                                                     as loanSupervisor,
       null                                                                     as groupVillageNumber,
       null                                                                     as cycleNumber,
       wela.INSTALL_COUNT                                                       as loanInstallment,
       'Irregular'                                                              as repaymentFrequency,
       wela.CURRENCY                                                            as currency,
       wela.ACC_OPEN_DT                                                         as contractDate,
       wela.ACC_LIMIT_AMN                                                       as orgSanctionedAmount,
       CASE
           WHEN wela.CURRENCY = 'USD'
               THEN wela.ACC_LIMIT_AMN
           ELSE NULL
           END                                                                  AS usdSanctionedAmount,
       CASE
           WHEN wela.CURRENCY = 'USD'
               THEN wela.ACC_LIMIT_AMN * 2500 -- <<< replace with your rate
           ELSE
               wela.ACC_LIMIT_AMN
           END                                                                  AS tzsSanctionedAmount,
       wela.TOT_DRAWDOWN_AMN                                                    as orgDisbursedAmount,
       CASE
           WHEN wela.CURRENCY = 'USD'
               THEN wela.TOT_DRAWDOWN_AMN
           ELSE NULL
           END                                                                  AS usdDisbursedAmount,
       CASE
           WHEN wela.CURRENCY = 'USD'
               THEN wela.TOT_DRAWDOWN_AMN * 2500 -- <<< replace with your rate
           ELSE
               wela.TOT_DRAWDOWN_AMN
           END                                                                  AS tzsDisbursedAmount,
       wela.DRAWDOWN_FST_DT                                                     AS disbursementDate,
       wela.ACC_EXP_DT                                                          AS maturityDate,
       COALESCE(wela.WRITE_OFF_DATE, wela.OV_EXP_DT, wela.ACC_EXP_DT)           AS realEndDate,
       (wela.NRM_BALANCE + wela.OV_BALANCE)                                     AS orgOutstandingPrincipalAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN (wela.NRM_BALANCE + wela.OV_BALANCE)
           ELSE NULL
           END                                                                  AS usdOutstandingPrincipalAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN (wela.NRM_BALANCE + wela.OV_BALANCE) * 2500
           ELSE (wela.NRM_BALANCE + wela.OV_BALANCE)
           END                                                                  AS tzsOutstandingPrincipalAmount,
       wela.INSTALLMENT_AMOUNT                                                  AS orgInstallmentAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.INSTALLMENT_AMOUNT
           ELSE NULL
           END                                                                  AS usdInstallmentAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.INSTALLMENT_AMOUNT * 2500
           ELSE wela.INSTALLMENT_AMOUNT
           END                                                                  AS tzsInstallmentAmount,
       wela.INSTA_PAID                                                          as loanInstallmentPaid,
       null                                                                     as gracePeriodPaymentPrincipal,
       wela.SELECTED_BANK_RATE                                                  AS primeLendingRate,
       null                                                                     AS interestPricingMethod,
       wela.FINAL_INTEREST                                                      AS annualInterestRate,
       null                                                                     AS effectiveAnnualInterestRate,
       null                                                                     AS loanFlagType,
       null                                                                     AS restructuringDate,
       wela.OVERDUE_DAYS                                                        AS pastDueDays,
       wela.OV_BALANCE                                                          AS pastDueAmount,
       null                                                                     AS internalRiskGroup,
       wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL AS orgAccruedInterestAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL
           ELSE NULL
           END                                                                  AS usdAccruedInterestAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN (wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL) *
                                           2500
           ELSE wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL
           END                                                                  AS tzsAccruedInterestAmount,

       wela.OV_RL_PNL_INT_BAL + wela.OV_URL_PNL_INT_BAL                         AS orgPenaltyChargedAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.OV_RL_PNL_INT_BAL + wela.OV_URL_PNL_INT_BAL
           ELSE NULL
           END                                                                  AS usdPenaltyChargedAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN (wela.OV_RL_PNL_INT_BAL + wela.OV_URL_PNL_INT_BAL) * 2500
           ELSE wela.OV_RL_PNL_INT_BAL + wela.OV_URL_PNL_INT_BAL
           END                                                                  AS tzsPenaltyChargedAmount,

       COALESCE(wela.TOT_PNL_INT_AMN, 0)                                        AS orgPenaltyPaidAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN COALESCE(wela.TOT_PNL_INT_AMN, 0)
           ELSE NULL
           END                                                                  AS usdPenaltyPaidAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN COALESCE(wela.TOT_PNL_INT_AMN, 0) * 2500
           ELSE COALESCE(wela.TOT_PNL_INT_AMN, 0)
           END                                                                  AS tzsPenaltyPaidAmount,
       wela.TOT_COMMISSION_AMN                                                  AS orgLoanFeesChargedAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_COMMISSION_AMN
           ELSE NULL
           END                                                                  AS usdLoanFeesChargedAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_COMMISSION_AMN * 2500
           ELSE wela.TOT_COMMISSION_AMN
           END                                                                  AS tzsLoanFeesChargedAmount,
       wela.TOT_EXPENSE_AMN                                                     AS orgLoanFeesPaidAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_EXPENSE_AMN
           ELSE NULL
           END                                                                  AS usdLoanFeesPaidAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_EXPENSE_AMN * 2500
           ELSE wela.TOT_EXPENSE_AMN
           END                                                                  AS tzsLoanFeesPaidAmount,
       wela.INSTALL_FIXED_AMN                                                   AS orgTotMonthlyPaymentAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.INSTALL_FIXED_AMN
           ELSE NULL
           END                                                                  AS usdTotMonthlyPaymentAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.INSTALL_FIXED_AMN * 2500
           ELSE wela.INSTALL_FIXED_AMN
           END                                                                  AS tzsTotMonthlyPaymentAmount,
       wela.CLOAN_CATEGORY_DESCRIPTION                                          AS sectorSnaClassification,
       'Current'                                                                AS assetClassificationCategory,
       wela.ACC_STATUS                                                          AS negStatusContract,
       wela.CUST_TYPE                                                           AS customerRole,
       wela.PROVISION_AMOUNT                                                    AS allowanceProbableLoss,
       wela.PROVISION_AMN                                                       AS botProvision,
       null                                                                     AS tradingIntent,
       wela.INTEREST_IN_SUSPENSE                                                AS orgSuspendedInterest,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.INTEREST_IN_SUSPENSE
           ELSE NULL
           END                                                                  AS usdSuspendedInterest,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.INTEREST_IN_SUSPENSE * 2500
           ELSE wela.INTEREST_IN_SUSPENSE
           END                                                                  AS tzsSuspendedInterest

from W_EOM_LOAN_ACCOUNT as wela
         LEFT JOIN CUSTOMER as c ON wela.CUST_ID = c.CUST_ID
         LEFT JOIN PROFITS_ACCOUNT pa
                   ON pa.CUST_ID = wela.CUST_ID
         LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
                                   id.fk_customercust_id = c.cust_id)
         LEFT JOIN LNS_CRD_CUST_DATA lccd on lccd.CUST_ID = wela.CUST_ID

         LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND
                                                 id.fkgd_has_been_issu = id_country.serial_num)
         LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE
         LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
         LEFT JOIN GENERIC_DETAIL GG
                   ON GG.FK_GENERIC_HEADPAR = wela.FKGH_HAS_AS_LOAN_P
                       AND GG.SERIAL_NUM = wela.FKGD_HAS_AS_LOAN_P

         LEFT JOIN LOAN_ACCOUNT L
                   ON wela.FK_UNITCODE = L.FK_UNITCODE
                       AND wela.ACC_TYPE = L.ACC_TYPE
                       AND wela.ACC_SN = L.ACC_SN
         LEFT JOIN LOAN_ADD_INFO N
                   ON N.ROW_ID = 1
                       AND wela.FK_UNITCODE = N.ACC_UNIT
                       AND wela.ACC_TYPE = N.ACC_TYPE
                       AND wela.ACC_SN = N.ACC_SN;