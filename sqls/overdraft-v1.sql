SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI')                        AS reportingDate,
       gte.CUST_ID,
       LTRIM(RTRIM(pa.ACCOUNT_NUMBER))                                            AS accountNumber,
       LTRIM(RTRIM(id.ID_NO))                                                     AS customerIdentificationNumber,
       TRIM(
               COALESCE(TRIM(c.FIRST_NAME), '') ||
               CASE
                   WHEN c.MIDDLE_NAME IS NOT NULL AND TRIM(c.MIDDLE_NAME) <> ''
                       THEN ' ' || c.MIDDLE_NAME
                   ELSE ''
                   END ||
               CASE
                   WHEN c.SURNAME IS NOT NULL AND TRIM(c.SURNAME) <> ''
                       THEN ' ' || c.SURNAME
                   ELSE ''
                   END
       )                                                                          AS clientName,
       ctl.CUSTOMER_TYPE                                                          as clientType,
       cl.COUNTRY_CODE                                                            as borrowerCountry,
       CAST(0 AS SMALLINT)                                                        as ratingStatus,
       CASE c.CUST_TYPE WHEN 2 THEN 'Unrated' END                                 as crRatingBorrower,
       CASE c.CUST_TYPE WHEN 2 THEN 'Grade B' END                                 as gradesUnratedBanks,
       null                                                                       as groupCode,
       null                                                                       as relatedEntityName,
       null                                                                       as relatedParty,
       null                                                                       as relationshipCategory,
       p.DESCRIPTION                                                              as loanProductType,
       gte.ID_PRODUCT                                                             as ID_PRODUCT,
       'OtherServices'                                                            as overdraftEconomicActivity,

       CASE la.ACC_STATUS
           WHEN '1' THEN 'Existing'
           WHEN '2' THEN 'TerminatedInAdvanceCorrectly'
           WHEN '3' THEN 'TerminatedAccordingTheContract'
           WHEN '4' THEN 'TerminatedInAdvanceIncorrectly'
           WHEN '5' THEN 'TerminatedInAdvanceIncorrectly'
           WHEN '6' THEN 'TerminatedInAdvanceIncorrectly'
           ELSE 'TerminatedAccordingTheContract'
           END                                                                    AS loanPhase,
       'NotSpecified'                                                             AS transferStatus,

       CASE p.DESCRIPTION
           WHEN 'STAFF CURRENT ACCOUNT OVERDRAFT' THEN 'StaffLoan'
           WHEN 'OVERDRAFT-CORPORATE' THEN 'Business'
           WHEN 'NGO/CLUB ACCOUNT OVERDRAFT' THEN 'Development'
           ELSE 'Other'
           END                                                                    AS purposeOtherLoans,
       la.ACC_OPEN_DT                                                             as contractDate,
       la.FK_UNITCODE                                                             as branchCode,
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
       )                                                                          AS loanOfficer,
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
       )                                                                          AS loanSupervisor,

       gte.CURRENCY_SHORT_DES                                                     as currency,
       ag.AGR_LIMIT                                                               as orgSanctionedAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN ag.AGR_LIMIT
           ELSE NULL
           END                                                                    AS usdSanctionedAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN ag.AGR_LIMIT * 2500 -- <<< replace with your rate
           ELSE
               ag.AGR_LIMIT
           END                                                                    AS tzsSanctionedAmount,
       la.TOT_DRAWDOWN_AMN                                                        as orgUtilisedAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN la.TOT_DRAWDOWN_AMN
           ELSE NULL
           END                                                                    AS usdUtilisedAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN la.TOT_DRAWDOWN_AMN * 2500 -- <<< replace with your rate
           ELSE
               la.TOT_DRAWDOWN_AMN
           END                                                                    AS tzsUtilisedAmount,
       gte.DC_AMOUNT                                                              as orgCrUsageLast30DaysAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN gte.DC_AMOUNT
           ELSE NULL
           END                                                                    AS usdCrUsageLast30DaysAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN gte.DC_AMOUNT * 2500 -- <<< replace with your rate
           ELSE
               gte.DC_AMOUNT
           END                                                                    AS tzsCrUsageLast30DaysAmount,
       la.DRAWDOWN_FST_DT                                                         AS disbursementDate,
       la.ACC_EXP_DT                                                              AS expiryDate,
       COALESCE(la.OV_EXP_DT, la.ACC_EXP_DT)                                      AS realEndDate,
       (la.NRM_CAP_BAL + la.OV_CAP_BAL)
           + (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) AS orgOutstandingAmount,

       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
               + (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
           ELSE NULL
           END                                                                    AS usdOutstandingAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
               + (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * 2500
           ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL)
               + (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
           END                                                                    AS tzsOutstandingAmount,
       (
           la.NRM_CAP_BAL + la.OV_CAP_BAL
           )                                                                      AS orgOutstandingPrincipalAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
           ELSE NULL
           END                                                                    AS usdOutstandingPrincipalAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 2500
           ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL)
           END                                                                    AS tzsOutstandingPrincipalAmount,
       la.DRAWDOWN_FST_DT                                                         AS latestCustomerCreditDate,
       la.DRAWDOWN_FST_AMN                                                        AS latestCreditAmount,
       la.INTER_RATE_SPRD                                                         AS primeLendingRate,
       la.MORATOR_NRM_RATE                                                        AS annualInterestRate,
       null                                                                       AS collateralPledged,
       null                                                                       AS orgCollateralValue,
       null                                                                       AS usdCollateralValue,
       null                                                                       AS tzsCollateralValue,
       0                                                                          AS restructuredLoans,
--        la.OVERDUE_DAYS                                                            AS pastDueDays,
--        la.OV_BALANCE                                                              AS pastDueAmount,
       la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL         AS orgAccruedInterestAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL
           ELSE NULL
           END                                                                    AS usdAccruedInterestAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN
               (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) *
               2500
           ELSE la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL
           END                                                                    AS tzsAccruedInterestAmount,

       la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL                               AS orgPenaltyChargedAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL
           ELSE NULL
           END                                                                    AS usdPenaltyChargedAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN (la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL) * 2500
           ELSE la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL
           END                                                                    AS tzsPenaltyChargedAmount,
       COALESCE(la.OV_RL_PNL_INT_BAL, 0)                                          AS orgPenaltyPaidAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN COALESCE(la.OV_RL_PNL_INT_BAL, 0)
           ELSE NULL
           END                                                                    AS usdPenaltyPaidAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN COALESCE(la.OV_RL_PNL_INT_BAL, 0) * 2500
           ELSE COALESCE(la.OV_RL_PNL_INT_BAL, 0)
           END                                                                    AS tzsPenaltyPaidAmount,


       la.TOT_COMMISSION_AMN                                                      AS orgLoanFeesChargedAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.TOT_COMMISSION_AMN
           ELSE NULL
           END                                                                    AS usdLoanFeesChargedAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.TOT_COMMISSION_AMN * 2500
           ELSE la.TOT_COMMISSION_AMN
           END                                                                    AS tzsLoanFeesChargedAmount,

       la.TOT_EXPENSE_AMN                                                         AS orgLoanFeesPaidAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.TOT_EXPENSE_AMN
           ELSE NULL
           END                                                                    AS usdLoanFeesPaidAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.TOT_EXPENSE_AMN * 2500
           ELSE la.TOT_EXPENSE_AMN
           END                                                                    AS tzsLoanFeesPaidAmount,

       0.00                                                                       AS orgTotMonthlyPaymentAmount,
       0.00                                                                       AS usdTotMonthlyPaymentAmount,
       0.00                                                                       AS tzsTotMonthlyPaymentAmount,

       la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN                                    AS orgInterestPaidTotal,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN
           ELSE NULL
           END                                                                    AS usdInterestPaidTotal,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN (la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN) * 2500
           ELSE la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN
           END                                                                    AS tzsInterestPaidTotal,
       'Current'                                                                  AS assetClassificationCategory,
       'Other financial Corporations'                                             AS sectorSnaClassification,
       la.ACC_STATUS                                                              AS negStatusContract,
       'N/A'                                                                      AS customerRole,
       0                                                                          AS allowanceProbableLoss,
       0                                                                          AS botProvision

FROM GLI_TRX_EXTRACT gte
         LEFT JOIN CUSTOMER c
                   ON gte.CUST_ID = c.CUST_ID
         LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE
         LEFT JOIN PROFITS_ACCOUNT pa
                   ON pa.CUST_ID = gte.CUST_ID
         LEFT JOIN PRODUCT p
                   ON p.ID_PRODUCT = gte.ID_PRODUCT
         LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
                                   id.fk_customercust_id = c.cust_id)

         LEFT JOIN cust_address ca
                   ON (ca.fk_customercust_id = c.cust_id AND ca.communication_addr = '1' AND
                       ca.entry_status = '1')
         LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND
                                                 id.fkgd_has_been_issu = id_country.serial_num)
         LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description


         LEFT JOIN LOAN_ACCOUNT la
                   ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
                       AND la.CUST_ID = gte.CUST_ID

         JOIN AGREEMENT ag
              ON ag.FK_UNITCODE = la.FK_AGREEMENTFK_UNI
                  AND ag.AGR_YEAR = la.FK_AGREEMENTAGR_YE
                  AND ag.AGR_SN = la.FK_AGREEMENTAGR_SN
                  AND ag.AGR_MEMBERSHIP_SN = la.FK_AGREEMENTAGR_ME
--
         LEFT JOIN BANKEMPLOYEE be
                   ON be.STAFF_NO = la.USR
WHERE gte.FK_GLG_ACCOUNTACCO IN ('1.1.0.05.0001', '1.1.0.05.0002', '1.1.0.05.0005')
;