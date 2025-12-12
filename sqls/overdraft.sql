SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI')                      AS reportingDate,
       LTRIM(RTRIM(pa.ACCOUNT_NUMBER))                                          AS accountNumber,
       LTRIM(RTRIM(id.ID_NO))                                                   AS customerIdentificationNumber,
       wela.CUSTOMER_NAME                                                       AS clientName,
       ctl.CUSTOMER_TYPE                                                        as clientType,
       cl.COUNTRY_CODE                                                          as borrowerCountry,
       null                                                                     as ratingStatus,
       null                                                                     as crRatingBorrower,
       null                                                                     as gradesUnratedBanks,
       null                                                                     as groupCode,
       null                                                                     as relatedEntityName,
       null                                                                     as relatedParty,
       null                                                                     as relationshipCategory,
       wela.LOAN_TYPE_NAME                                                      as loanProductType,
       'OtherServices'                                                          as overdraftEconomicActivity,
       'Existing'                                                               as loanPhase,
       'NotSpecified'                                                           as transferStatus,
       CASE wela.PRODUCT_DESC
           WHEN 'STAFF PERSONAL LOANS' THEN 'STAFF LOAN'
           WHEN 'OVERDRAFT BUSINESS' THEN 'BUSINESS OVERDRAFT'
           WHEN 'TRADE FINANCE' THEN 'TRADE FINANCE'
           ELSE 'Other'
           END                                                                  AS purposeOtherLoans,
       wela.ACC_OPEN_DT                                                         as contractDate,
       wela.FK_UNITCODE                                                         as branchCode,
       wela.LOAN_OFFICER_NAME                                                   as loanOfficer,
       null                                                                     as loanSupervisor,
       wela.CURRENCY                                                            as currency,
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
       wela.TOT_DRAWDOWN_AMN                                                    as orgUtilisedAmount,
       CASE
           WHEN wela.CURRENCY = 'USD'
               THEN wela.TOT_DRAWDOWN_AMN
           ELSE NULL
           END                                                                  AS usdUtilisedAmount,
       CASE
           WHEN wela.CURRENCY = 'USD'
               THEN wela.TOT_DRAWDOWN_AMN * 2500 -- <<< replace with your rate
           ELSE
               wela.TOT_DRAWDOWN_AMN
           END                                                                  AS tzsUtilisedAmount,
       wela.TRX_AMN                                                             as orgCrUsageLast30DaysAmount,
       CASE
           WHEN wela.CURRENCY = 'USD'
               THEN wela.TRX_AMN
           ELSE NULL
           END                                                                  AS usdCrUsageLast30DaysAmount,
       CASE
           WHEN wela.CURRENCY = 'USD'
               THEN wela.TRX_AMN * 2500 -- <<< replace with your rate
           ELSE
               wela.TRX_AMN
           END                                                                  AS tzsCrUsageLast30DaysAmount,
       wela.DRAWDOWN_FST_DT                                                     AS disbursementDate,
       wela.ACC_EXP_DT                                                          AS expiryDate,

       COALESCE(wela.WRITE_OFF_DATE, wela.OV_EXP_DT, wela.ACC_EXP_DT)           AS realEndDate,
       (wela.NRM_BALANCE + wela.OV_BALANCE)                                     AS orgOutstandingAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN (wela.NRM_BALANCE + wela.OV_BALANCE)
           ELSE NULL
           END                                                                  AS usdOutstandingAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN (wela.NRM_BALANCE + wela.OV_BALANCE) * 2500
           ELSE (wela.NRM_BALANCE + wela.OV_BALANCE)
           END                                                                  AS tzsOutstandingAmount,

       wela.DRAWDOWN_FST_DT                                                     AS latestCustomerCreditDate,
       wela.DRAWDOWN_FST_AMN                                                    AS latestCreditAmount,
       wela.SELECTED_BANK_RATE                                                  AS primeLendingRate,
       wela.FINAL_INTEREST                                                      AS annualInterestRate,
       wela.COLLATERAL_OM_VALUE                                                 AS collateralPledged,
       wela.COLLATERAL_OM_VALUE                                                 AS orgCollateralValue,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.COLLATERAL_OM_VALUE
           ELSE NULL
           END                                                                  AS usdCollateralValue,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.COLLATERAL_OM_VALUE * 2500
           ELSE wela.COLLATERAL_OM_VALUE
           END                                                                  AS tzsCollateralValue,

       CASE WHEN wela.ACC_DRAWDOWN_STS = 'R' THEN 1 ELSE 0 END                  AS restructuredLoans,
       wela.OVERDUE_DAYS                                                        AS pastDueDays,
       wela.OV_BALANCE                                                          AS pastDueAmount,
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

       wela.TOT_NRM_INT_AMN + wela.TOT_PNL_INT_AMN                              AS orgInterestPaidTotal,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_NRM_INT_AMN + wela.TOT_PNL_INT_AMN
           ELSE NULL
           END                                                                  AS usdInterestPaidTotal,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN (wela.TOT_NRM_INT_AMN + wela.TOT_PNL_INT_AMN) * 2500
           ELSE wela.TOT_NRM_INT_AMN + wela.TOT_PNL_INT_AMN
           END                                                                  AS tzsInterestPaidTotal,

       'Current'                                                                AS assetClassificationCategory,
       wela.CLOAN_CATEGORY_DESCRIPTION                                          AS sectorSnaClassification,
       wela.ACC_STATUS                                                          AS negStatusContract,
       wela.CUST_TYPE                                                           AS customerRole,
       wela.PROVISION_AMOUNT                                                    AS allowanceProbableLoss,
       wela.PROVISION_AMN                                                       AS botProvision

FROM W_EOM_LOAN_ACCOUNT wela
         LEFT JOIN CUSTOMER c
                   ON wela.CUST_ID = c.CUST_ID
         LEFT JOIN PROFITS_ACCOUNT pa
                   ON pa.CUST_ID = wela.CUST_ID
         LEFT JOIN PRODUCT p
                   ON p.ID_PRODUCT = wela.ID_PRODUCT
         LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
                                   id.fk_customercust_id = c.cust_id)
         LEFT JOIN cust_address ca
                   ON (ca.fk_customercust_id = c.cust_id AND ca.communication_addr = '1' AND
                       ca.entry_status = '1')
         LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND
                                                 id.fkgd_has_been_issu = id_country.serial_num)
         LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE
         LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
--             WHERE pa.EXTERNAL_GLACCOUNT IN ('110050001','110050002')
WHERE wela.EOM_DATE >= CURRENT DATE - 300 DAYS
  and wela.OVERDRAFT_TYPE_FLAG = 'Overdraft'
    FETCH FIRST 50 ROWS ONLY;