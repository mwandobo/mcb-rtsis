WITH txn_summary AS (
    SELECT
        ACCOUNT_NUMBER,
        CUST_ID,
--         CURRENCY,
        SUM(FC_AMOUNT) AS fc_balance,
        SUM(DC_AMOUNT) AS dc_balance
--         SUM(NRM_ACR_INT_BAL + OV_ACR_NRM_INT_BAL + OV_ACR_PNL_INT_BAL) AS accrued_interest,
--         SUM(INTEREST_IN_SUSPENSE) AS suspended_interest
    FROM GLI_TRX_EXTRACT
    WHERE FK_GLG_ACCOUNTACCO IN ('7.0.5.19.0001', '7.0.5.19.0002', '7.0.5.19.0003')
      AND TRN_DATE <= CURRENT_DATE
    GROUP BY ACCOUNT_NUMBER, CUST_ID

)
SELECT
    CURRENT_TIMESTAMP AS reportingDate,
--     LTRIM(RTRIM(id.ID_NO)) AS borrowersInstitutionCode,
    'TANZANIA, UNITED REPUBLIC OF' AS borrowerCountry,
    'Domestic banks unrelated' AS relationshipType,
    txn.ACCOUNT_NUMBER AS loanNumber,
    'Interbank call loans in Tanzania' AS loanType
--     la.ACC_OPEN_DT AS issueDate,
--     la.ACC_EXP_DT AS loanMaturityDate,
--     txn.CURRENCY AS currency,
--     la.ACC_LIMIT_AMN AS orgLoanAmount
--     CASE WHEN txn.CURRENCY = 'USD' THEN la.ACC_LIMIT_AMN ELSE NULL END AS usdLoanAmount,
--     CASE WHEN txn.CURRENCY = 'USD' THEN la.ACC_LIMIT_AMN * 2500 ELSE la.ACC_LIMIT_AMN END AS tzsLoanAmount
--     la.FINAL_INTEREST AS interestRate,
--     txn.accrued_interest AS orgAccruedInterestAmount,
--     CASE WHEN txn.CURRENCY = 'USD' THEN txn.accrued_interest ELSE NULL END AS usdAccruedInterestAmount,
--     CASE WHEN txn.CURRENCY = 'USD' THEN txn.accrued_interest * 2500 ELSE txn.accrued_interest END AS tzsAccruedInterestAmount,
--     txn.suspended_interest AS orgSuspendedInterest,
--     CASE WHEN txn.CURRENCY = 'USD' THEN txn.suspended_interest ELSE NULL END AS usdSuspendedInterest,
--     CASE WHEN txn.CURRENCY = 'USD' THEN txn.suspended_interest * 2500 ELSE txn.suspended_interest END AS tzsSuspendedInterest
FROM txn_summary txn
--          LEFT JOIN LOAN_ACCOUNT la ON txn.ACCOUNT_NUMBER = la.OLD_ACCOUNT_CD
--          LEFT JOIN CUSTOMER c ON txn.CUST_ID = c.CUST_ID
--          LEFT JOIN other_id id ON id.fk_customercust_id = c.CUST_ID AND id.main_flag = '1'
-- ;
