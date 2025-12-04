-- Overdraft RTSIS extract (DB2) - change only the EOM_DATE in PARAMS
WITH PARAMS AS (
    SELECT DATE('2025-11-30') AS EOM_DATE    -- <<-- change this only
    FROM SYSIBM.SYSDUMMY1
),

-- FX rates (assumes a FX_RATES table: RATE_DATE, CURRENCY, TO_CURR, RATE)
FX_USD AS (
    SELECT CURRENCY, RATE
    FROM FX_RATES
    WHERE RATE_DATE = (SELECT EOM_DATE FROM PARAMS) AND TO_CURR = 'USD'
),
FX_TZS AS (
    SELECT CURRENCY, RATE
    FROM FX_RATES
    WHERE RATE_DATE = (SELECT EOM_DATE FROM PARAMS) AND TO_CURR = 'TZS'
),

-- Credit usage last 30 days (sum of withdrawals/usage). 
-- Adjust TRX_DIRECTION/TRX_TYPE filter as per GLI_TRX_EXTRACT semantics.
CR_USAGE_30 AS (
    SELECT 
        TRIM(T.ACCOUNT_NO) AS ACCOUNT_NO,
        SUM(CASE WHEN T.AMOUNT IS NULL THEN 0 ELSE T.AMOUNT END) AS ORG_CR_USAGE_30
    FROM GLI_TRX_EXTRACT T
    WHERE T.TRX_DATE BETWEEN (SELECT EOM_DATE FROM PARAMS) - 30 DAYS
                         AND (SELECT EOM_DATE FROM PARAMS)
      AND ( -- example filter: treat negative/withdrawal as usage. Adjust to your data.
            UPPER(COALESCE(T.TRX_DIRECTION,'DR')) = 'DR' 
         OR UPPER(COALESCE(T.TRX_TYPE,'')) IN ('WITHDRAWAL','DEBIT')
      )
    GROUP BY TRIM(T.ACCOUNT_NO)
),

-- Last credit per account (latest credit date & amount)
LAST_CREDIT AS (
    SELECT 
        TRIM(T.ACCOUNT_NO) AS ACCOUNT_NO,
        MAX(CASE WHEN COALESCE(T.AMOUNT,0) > 0 THEN T.TRX_DATE END) AS LAST_CREDIT_DATE,
        -- Grab amount of latest credit via window
        MAX(CASE WHEN t2.rn = 1 THEN t2.amount END) AS LAST_CREDIT_AMT
    FROM (
        SELECT T.*,
               ROW_NUMBER() OVER (PARTITION BY TRIM(T.ACCOUNT_NO) ORDER BY T.TRX_DATE DESC, T.ROW_ID DESC) AS rn
        FROM GLI_TRX_EXTRACT T
        WHERE T.AMOUNT > 0
          AND T.TRX_DATE <= (SELECT EOM_DATE FROM PARAMS)
    ) t2
    GROUP BY TRIM(t2.ACCOUNT_NO)
),

-- Collateral pre-aggregation (if applicable)
COLLATERAL AS (
    SELECT PRFT_ACCOUNT,
           SUM(EST_VALUE_AMN) AS ORG_COLL_VALUE_SUM
    FROM EOM_ACCOUNT_COLLATERAL
    WHERE EOM_DATE = (SELECT EOM_DATE FROM PARAMS)
    GROUP BY PRFT_ACCOUNT
)

SELECT
    -- 1 Reporting Date and Time
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI')                         AS reportingDate,

    -- 2 account Number
    LTRIM(RTRIM(W.ACCOUNT_NO))                                                  AS accountNumber,

    -- 3 client Identification Number
    LTRIM(RTRIM(W.CUST_ID))                                                     AS customerIdentificationNumber,

    -- 4 client Name (FIRST + MIDDLE + SURNAME)
    RTRIM(LTRIM(
        COALESCE(S.FIRST_NAME,'') || ' ' ||
        COALESCE(S.MIDDLE_NAME,'') || ' ' ||
        COALESCE(S.SURNAME,'')
    ))                                                                          AS clientName,

    -- 5 client Type (lookup D54 assumed: LOOKUP_D54)
    COALESCE(d54.lookup_value, W.CUST_TYPE)                                     AS clientType,

    -- 6 borrower Country
    COALESCE(C.COUNTRY_CODE, 'TZA')                                              AS borrowerCountry,

    -- 7 rating status (boolean Y/N)
    CASE WHEN TRIM(C.RATING) IS NOT NULL AND TRIM(C.RATING) <> '' THEN 'Y' ELSE 'N' END
                                                                               AS ratingStatus,

    -- 8 Credit Rating Borrower (D67)
    COALESCE(d67.lookup_value, NULL)                                            AS crRatingBorrower,

    -- 9 grades for Unrated Banks (D68)
    COALESCE(d68.lookup_value, NULL)                                            AS gradesUnratedBanks,

    -- 10 groupCode (optional)
    COALESCE(W.GROUP_CODE, NULL)                                                 AS groupCode,

    -- 11 relatedEntityName
    COALESCE(W.RELATED_ENTITY_NAME, NULL)                                        AS relatedEntityName,

    -- 12 relatedParty (D55)
    COALESCE(d55.lookup_value, NULL)                                             AS relatedParty,

    -- 13 relationshipCategory (D95)
    COALESCE(d95.lookup_value, NULL)                                             AS relationshipCategory,

    -- 14 loanProductType
    LTRIM(RTRIM(W.PRODUCT_DESC))                                                AS loanProductType,

    -- 15 overdraftEconomicActivity (lookup)
    COALESCE(d_econ.lookup_value, NULL)                                          AS overdraftEconomicActivity,

    -- 16 loanPhase (D131)
    COALESCE(d131.lookup_value, NULL)                                            AS loanPhase,

    -- 17 transferStatus (D132)
    COALESCE(d132.lookup_value, NULL)                                            AS transferStatus,

    -- 18 purposeOtherLoans (D133)
    COALESCE(d133.lookup_value, NULL)                                            AS purposeOtherLoans,

    -- 19 contractDate (use limit creation or account open)
    VARCHAR_FORMAT(COALESCE(DP.LIMIT_START_DATE, W.ACC_OPEN_DT), 'DDMMYYYYHH24MI') AS contractDate,

    -- 20 branchCode
    LTRIM(RTRIM(W.FK_UNITCODE))                                                  AS branchCode,

    -- 21 loanOfficer
    RTRIM(LTRIM(W.LOAN_OFFICER_NAME))                                            AS loanOfficer,

    -- 22 loanSupervisor
    RTRIM(LTRIM(W.LOAN_SUPERVISOR))                                              AS loanSupervisor,

    -- 23 currency
    LTRIM(RTRIM(W.CURRENCY))                                                     AS currency,

    -- 24 orgSanctionedAmount (from limits table EOM_DEPOSITS)
    CAST(COALESCE(DP.LIMIT_AMT, 0) AS DECFLOAT)                                  AS orgSanctionedAmount,

    -- 25 usdSanctionedAmount (convert using FX_USD)
    CAST(
      CASE 
        WHEN LTRIM(RTRIM(W.CURRENCY)) = 'USD' THEN COALESCE(DP.LIMIT_AMT,0)
        ELSE COALESCE(DP.LIMIT_AMT,0) * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY), 0)
      END AS DECFLOAT)                                                            AS usdSanctionedAmount,

    -- 26 tzsSanctionedAmount
    CAST(
      CASE 
        WHEN LTRIM(RTRIM(W.CURRENCY)) = 'TZS' THEN COALESCE(DP.LIMIT_AMT,0)
        ELSE COALESCE(DP.LIMIT_AMT,0) * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY), 0)
      END AS DECFLOAT)                                                            AS tzsSanctionedAmount,

    -- 27 orgUtilisedAmount (use outstanding or utilized from EOM_DEPOSITS or W fields)
    CAST(COALESCE(DP.CURRENT_UTILISED_AMT, COALESCE(W.NRM_CAP_BAL,0) + COALESCE(W.OV_CAP_BAL,0)) AS DECFLOAT)
                                                                                AS orgUtilisedAmount,

    -- 28 usdUtilisedAmount
    CAST(
      CASE WHEN LTRIM(RTRIM(W.CURRENCY)) = 'USD' THEN COALESCE(DP.CURRENT_UTILISED_AMT, COALESCE(W.NRM_CAP_BAL,0)+COALESCE(W.OV_CAP_BAL,0))
           ELSE COALESCE(DP.CURRENT_UTILISED_AMT, COALESCE(W.NRM_CAP_BAL,0)+COALESCE(W.OV_CAP_BAL,0))
             * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY),0)
      END AS DECFLOAT)                                                            AS usdUtilisedAmount,

    -- 29 tzsUtilisedAmount
    CAST(
      CASE WHEN LTRIM(RTRIM(W.CURRENCY)) = 'TZS' THEN COALESCE(DP.CURRENT_UTILISED_AMT, COALESCE(W.NRM_CAP_BAL,0)+COALESCE(W.OV_CAP_BAL,0))
           ELSE COALESCE(DP.CURRENT_UTILISED_AMT, COALESCE(W.NRM_CAP_BAL,0)+COALESCE(W.OV_CAP_BAL,0))
             * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY),0)
      END AS DECFLOAT)                                                            AS tzsUtilisedAmount,

    -- 30 orgCrUsageLast30DaysAmount (from CR_USAGE_30)
    CAST(COALESCE(CU30.ORG_CR_USAGE_30,0) AS DECFLOAT)                            AS orgCrUsageLast30DaysAmount,

    -- 31 usdCrUsageLast30DaysAmount
    CAST(COALESCE(CU30.ORG_CR_USAGE_30,0) * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT)
                                                                                AS usdCrUsageLast30DaysAmount,

    -- 32 tzsCrUsageLast30DaysAmount
    CAST(COALESCE(CU30.ORG_CR_USAGE_30,0) * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT)
                                                                                AS tzsCrUsageLast30DaysAmount,

    -- 33 disbursementDate (limit start)
    VARCHAR_FORMAT(DP.LIMIT_START_DATE, 'DDMMYYYYHH24MI')                         AS disbursementDate,

    -- 34 expiryDate
    VARCHAR_FORMAT(DP.LIMIT_EXPIRY_DATE, 'DDMMYYYYHH24MI')                         AS expiryDate,

    -- 35 realEndDate (if present)
    VARCHAR_FORMAT(DP.REAL_END_DATE, 'DDMMYYYYHH24MI')                             AS realEndDate,

    -- 36 orgOutstandingAmount (principal + interest) - prefer W.OUTSTANDING_BAL if present
    CAST(COALESCE(W.OUTSTANDING_BAL, COALESCE(W.NRM_CAP_BAL,0) + COALESCE(W.OV_CAP_BAL,0)) AS DECFLOAT)
                                                                                AS orgOutstandingAmount,

    -- 37 usdOutstandingAmount
    CAST(
      CASE WHEN LTRIM(RTRIM(W.CURRENCY)) = 'USD' THEN COALESCE(W.OUTSTANDING_BAL, COALESCE(W.NRM_CAP_BAL,0)+COALESCE(W.OV_CAP_BAL,0))
           ELSE COALESCE(W.OUTSTANDING_BAL, COALESCE(W.NRM_CAP_BAL,0)+COALESCE(W.OV_CAP_BAL,0))
                * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY),0)
      END AS DECFLOAT)                                                            AS usdOutstandingAmount,

    -- 38 tzsOutstandingAmount
    CAST(
      CASE WHEN LTRIM(RTRIM(W.CURRENCY)) = 'TZS' THEN COALESCE(W.OUTSTANDING_BAL, COALESCE(W.NRM_CAP_BAL,0)+COALESCE(W.OV_CAP_BAL,0))
           ELSE COALESCE(W.OUTSTANDING_BAL, COALESCE(W.NRM_CAP_BAL,0)+COALESCE(W.OV_CAP_BAL,0))
                * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY),0)
      END AS DECFLOAT)                                                            AS tzsOutstandingAmount,

    -- 39 orgOutstandingPrincipalAmount (assume NRM_CAP_BAL is principal)
    CAST(COALESCE(W.NRM_CAP_BAL,0) AS DECFLOAT)                                   AS orgOutstandingPrincipalAmount,

    -- 40 usdOutstandingPrincipalAmount
    CAST(COALESCE(W.NRM_CAP_BAL,0) * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT)
                                                                                AS usdOutstandingPrincipalAmount,

    -- 41 tzsOutstandingPrincipalAmount
    CAST(COALESCE(W.NRM_CAP_BAL,0) * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT)
                                                                                AS tzsOutstandingPrincipalAmount,

    -- 42 latestCustomerCreditDate & 43 latestCreditAmount (from LAST_CREDIT)
    VARCHAR_FORMAT(LAST_CR.LAST_CREDIT_DATE, 'DDMMYYYYHH24MI')                    AS latestCustomerCreditDate,
    CAST(COALESCE(LAST_CR.LAST_CREDIT_AMT,0) AS DECFLOAT)                         AS latestCreditAmount,

    -- 44 primeLendingRate & 45 annualInterestRate (use bank params or W fields)
    CAST(COALESCE(W.PRIME_LENDING_RATE, B.PRIME_RATE) AS DECFLOAT)                AS primeLendingRate,
    CAST(COALESCE(W.ANNUAL_INTEREST_RATE, 0) AS DECFLOAT)                        AS annualInterestRate,

    -- 46-49 Collateral values (pre-agg)
    CAST(COALESCE(COLL.ORG_COLL_VALUE_SUM,0) AS DECFLOAT)                         AS orgCollateralValue,
    CAST(COALESCE(COLL.ORG_COLL_VALUE_SUM,0) * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT)
                                                                                AS usdCollateralValue,
    CAST(COALESCE(COLL.ORG_COLL_VALUE_SUM,0) * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT)
                                                                                AS tzsCollateralValue,

    -- 50 restructuredLoans
    CASE WHEN COALESCE(W.RESTRUCTURED_FLAG,'N') = 'Y' THEN 'Y' ELSE 'N' END       AS restructuredLoans,

    -- 51 pastDueDays & pastDueAmount
    COALESCE(W.PAST_DUE_DAYS, 0)                                                 AS pastDueDays,
    CAST(COALESCE(W.PAST_DUE_PRINCIPAL, 0) AS DECFLOAT)                          AS pastDueAmount,

    -- 52 Accrued interest & conversions
    CAST(COALESCE(W.NRM_ACCRUAL_AMN,0) AS DECFLOAT)                              AS orgAccruedInterestAmount,
    CAST(COALESCE(W.NRM_ACCRUAL_AMN,0) * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT)
                                                                                AS usdAccruedInterestAmount,
    CAST(COALESCE(W.NRM_ACCRUAL_AMN,0) * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT)
                                                                                AS tzsAccruedInterestAmount,

    -- 53-64 Penalties, fees, monthly totals, payments, interest paid etc.
    CAST(COALESCE(W.PENALTY_CHARGED_AMN,0) AS DECFLOAT)                           AS orgPenaltyChargedAmount,
    CAST(COALESCE(W.PENALTY_CHARGED_AMN,0) * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT) AS usdPenaltyChargedAmount,
    CAST(COALESCE(W.PENALTY_CHARGED_AMN,0) * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT) AS tzsPenaltyChargedAmount,

    CAST(COALESCE(W.PENALTY_PAID_AMN,0) AS DECFLOAT)                              AS orgPenaltyPaidAmount,
    CAST(COALESCE(W.PENALTY_PAID_AMN,0) * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT) AS usdPenaltyPaidAmount,
    CAST(COALESCE(W.PENALTY_PAID_AMN,0) * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT) AS tzsPenaltyPaidAmount,

    CAST(COALESCE(W.FEE_CHARGED_AMN,0) AS DECFLOAT)                               AS orgLoanFeesChargedAmount,
    CAST(COALESCE(W.FEE_CHARGED_AMN,0) * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT) AS usdLoanFeesChargedAmount,
    CAST(COALESCE(W.FEE_CHARGED_AMN,0) * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT) AS tzsLoanFeesChargedAmount,

    CAST(COALESCE(W.FEE_PAID_AMN,0) AS DECFLOAT)                                  AS orgLoanFeesPaidAmount,
    CAST(COALESCE(W.FEE_PAID_AMN,0) * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT) AS usdLoanFeesPaidAmount,
    CAST(COALESCE(W.FEE_PAID_AMN,0) * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT) AS tzsLoanFeesPaidAmount,

    CAST(COALESCE(W.TOT_MONTHLY_PAYMENT_AMN,0) AS DECFLOAT)                       AS orgTotMonthlyPaymentAmount,
    CAST(COALESCE(W.TOT_MONTHLY_PAYMENT_AMN,0) * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT) AS usdTotMonthlyPaymentAmount,
    CAST(COALESCE(W.TOT_MONTHLY_PAYMENT_AMN,0) * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT) AS tzsTotMonthlyPaymentAmount,

    CAST(COALESCE(W.INTEREST_PAID_MONTH_AMN,0) AS DECFLOAT)                       AS orgInterestPaidTotal,
    CAST(COALESCE(W.INTEREST_PAID_MONTH_AMN,0) * COALESCE((SELECT RATE FROM FX_USD f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT) AS usdInterestPaidTotal,
    CAST(COALESCE(W.INTEREST_PAID_MONTH_AMN,0) * COALESCE((SELECT RATE FROM FX_TZS f WHERE f.CURRENCY = W.CURRENCY),0) AS DECFLOAT) AS tzsInterestPaidTotal,

    -- 65 asset & sector & negative status & role
    COALESCE(W.ASSET_CLASS_CATEGORY, d032.lookup_value)                          AS assetClassificationCategory,
    COALESCE(W.SECTOR_SNACODE, NULL)                                             AS sectorSnaClassification,
    COALESCE(d147.lookup_value, NULL)                                            AS negStatusContract,
    COALESCE(W.CUSTOMER_ROLE, NULL)                                              AS customerRole,

    -- 66 allowance & bot provision
    CAST(COALESCE(W.PROVISION_AMOUNT,0) AS DECFLOAT)                             AS allowanceProbableLoss,
    CAST(COALESCE(W.BOT_PROVISION_AMT,0) AS DECFLOAT)                             AS botProvision

FROM W_EOM_ACCOUNT W
CROSS JOIN PARAMS

LEFT JOIN EOM_DEPOSITS DP
    ON TRIM(DP.ACCOUNT_NO) = TRIM(W.ACCOUNT_NO)    -- ensure correct join columns; adjust if different

LEFT JOIN CUSTOMER S
    ON W.CUST_ID = S.CUST_ID

LEFT JOIN W_DIM_CUSTOMER C
    ON W.CUST_ID = C.CUST_ID
   AND W.EOM_DATE BETWEEN C.ROW_START_DATE AND C.ROW_END_DATE

LEFT JOIN CR_USAGE_30 CU30
    ON CU30.ACCOUNT_NO = TRIM(W.ACCOUNT_NO)

LEFT JOIN LAST_CREDIT LAST_CR
    ON LAST_CR.ACCOUNT_NO = TRIM(W.ACCOUNT_NO)

LEFT JOIN COLLATERAL COLL
    ON COLL.PRFT_ACCOUNT = W.ACCOUNT_NO

LEFT JOIN BANK_PARAMETERS B
    ON 1 = 1

-- Lookups (replace with your actual lookup table names)
LEFT JOIN LOOKUP_D54 d54 ON d54.CODE = W.CUST_TYPE
LEFT JOIN LOOKUP_D67 d67 ON d67.CODE = C.CREDIT_RATING_CODE
LEFT JOIN LOOKUP_D68 d68 ON d68.CODE = W.GRADE_UNRATED_BANKS
LEFT JOIN LOOKUP_D131 d131 ON d131.CODE = W.LOAN_PHASE
LEFT JOIN LOOKUP_D132 d132 ON d132.CODE = W.TRANSFER_STATUS
LEFT JOIN LOOKUP_D133 d133 ON d133.CODE = W.PURPOSE_CODE
LEFT JOIN LOOKUP_D032 d032 ON d032.CODE = W.ASSET_CLASS_CATEGORY
LEFT JOIN LOOKUP_D147 d147 ON d147.CODE = W.NEG_STATUS_CODE
LEFT JOIN LOOKUP_D55 d55 ON d55.CODE = W.RELATED_PARTY_CODE
LEFT JOIN LOOKUP_ECON d_econ ON d_econ.CODE = W.ECON_ACTIVITY_CODE

WHERE W.EOM_DATE = (SELECT EOM_DATE FROM PARAMS)

ORDER BY W.ACCOUNT_NO;
