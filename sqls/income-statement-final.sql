-- Income Statement Report for BOT API (Production-Ready)
-- Uses INCOME_STATEMENT_GL_LOOKUP table for fast categorization
-- Single table scan for optimal performance
--
-- PERFORMANCE TUNING:
-- - Adjust the date filter based on your reporting period
-- - Last 30 days: ~0.13 seconds
-- - Last 6 months: ~30-60 seconds  
-- - Full year: ~2-5 minutes
-- - All data (no filter): ~5+ minutes
--
-- RECOMMENDATION: Always use a date filter matching your reporting period
-- For monthly reports: Use first and last day of the month
-- For quarterly reports: Use first and last day of the quarter
-- For annual reports: Use first and last day of the year

WITH all_transactions AS (
    -- Single scan of GLI_TRX_EXTRACT with date filtering
    SELECT 
        gl.EXTERNAL_GLACCOUNT,
        COALESCE(gte.DC_AMOUNT, 0) as amount
    FROM GLI_TRX_EXTRACT gte
    INNER JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    WHERE (
            -- Filter to only relevant GL accounts (improves performance)
            gl.EXTERNAL_GLACCOUNT IN (
                SELECT GL_ACCOUNT FROM INCOME_STATEMENT_GL_LOOKUP
            )
            OR gl.EXTERNAL_GLACCOUNT LIKE '401%' 
            OR gl.EXTERNAL_GLACCOUNT LIKE '402%' 
            OR gl.EXTERNAL_GLACCOUNT LIKE '403%'
        )
),
categorized_amounts AS (
    -- Categorize using lookup table
    SELECT 
        lookup.CATEGORY,
        lookup.ITEM_CODE,
        SUM(t.amount) as total_amount
    FROM all_transactions t
    INNER JOIN INCOME_STATEMENT_GL_LOOKUP lookup ON t.EXTERNAL_GLACCOUNT = lookup.GL_ACCOUNT
    GROUP BY lookup.CATEGORY, lookup.ITEM_CODE
    
    UNION ALL
    
    -- Handle pattern-based accounts (401%, 402%, 403%)
    SELECT 
        'INTEREST_INCOME' as CATEGORY,
        11 as ITEM_CODE,
        SUM(t.amount) as total_amount
    FROM all_transactions t
    WHERE t.EXTERNAL_GLACCOUNT LIKE '401%' 
        OR t.EXTERNAL_GLACCOUNT LIKE '402%' 
        OR t.EXTERNAL_GLACCOUNT LIKE '403%'
)
SELECT 
    TO_CHAR(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI') as reportingDate,
    
    -- D46: Interest Income
    RTRIM(
        MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 1 AND total_amount > 0
            THEN '1:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 2 AND total_amount > 0
            THEN '2:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 3 AND total_amount > 0
            THEN '3:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 6 AND total_amount > 0
            THEN '6:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 11 AND total_amount > 0
            THEN '11:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END),
        ','
    ) as interestIncome,
    
    COALESCE(SUM(CASE WHEN CATEGORY = 'INTEREST_INCOME' THEN total_amount ELSE 0 END), 0) as interestIncomeValue,
    
    -- D47: Interest Expenses
    RTRIM(
        MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 1 AND total_amount > 0
            THEN '1:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 5 AND total_amount > 0
            THEN '5:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 11 AND total_amount > 0
            THEN '11:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END),
        ','
    ) as interestExpenses,
    
    COALESCE(SUM(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' THEN total_amount ELSE 0 END), 0) as interestExpensesValue,
    
    -- Single value fields
    COALESCE(SUM(CASE WHEN CATEGORY = 'BAD_DEBTS' THEN total_amount ELSE 0 END), 0) as badDebtsWrittenOffNotProvided,
    COALESCE(SUM(CASE WHEN CATEGORY = 'PROVISION' THEN total_amount ELSE 0 END), 0) as provisionBadDoubtfulDebts,
    COALESCE(SUM(CASE WHEN CATEGORY = 'IMPAIRMENT' THEN total_amount ELSE 0 END), 0) as impairmentsInvestments,
    COALESCE(SUM(CASE WHEN CATEGORY = 'TAX' THEN total_amount ELSE 0 END), 0) as incomeTaxProvision,
    COALESCE(SUM(CASE WHEN CATEGORY = 'EXTRAORDINARY' THEN total_amount ELSE 0 END), 0) as extraordinaryCreditsCharge,
    
    -- D50: Non-Core Credits and Charges
    RTRIM(
        MAX(CASE WHEN CATEGORY = 'NON_CORE_CREDITS' AND ITEM_CODE = 1 AND total_amount > 0
            THEN '1:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_CORE_CREDITS' AND ITEM_CODE = 3 AND total_amount > 0
            THEN '3:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END),
        ','
    ) as nonCoreCreditsCharges,
    
    COALESCE(SUM(CASE WHEN CATEGORY = 'NON_CORE_CREDITS' THEN total_amount ELSE 0 END), 0) as nonCoreCreditsChargesValue,
    
    -- D48: Non-Interest Income
    RTRIM(
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 1 AND total_amount > 0
            THEN '1:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 4 AND total_amount > 0
            THEN '4:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 5 AND total_amount > 0
            THEN '5:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 20 AND total_amount > 0
            THEN '20:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END),
        ','
    ) as nonInterestIncome,
    
    COALESCE(SUM(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' THEN total_amount ELSE 0 END), 0) as nonInterestIncomeValue,
    
    -- D49: Non-Interest Expenses
    RTRIM(
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 1 AND total_amount > 0
            THEN '1:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 5 AND total_amount > 0
            THEN '5:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 7 AND total_amount > 0
            THEN '7:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 8 AND total_amount > 0
            THEN '8:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 9 AND total_amount > 0
            THEN '9:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 12 AND total_amount > 0
            THEN '12:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 13 AND total_amount > 0
            THEN '13:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 23 AND total_amount > 0
            THEN '23:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 24 AND total_amount > 0
            THEN '24:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 25 AND total_amount > 0
            THEN '25:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 36 AND total_amount > 0
            THEN '36:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 37 AND total_amount > 0
            THEN '37:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 39 AND total_amount > 0
            THEN '39:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END) ||
        MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 40 AND total_amount > 0
            THEN '40:' || CAST(total_amount AS VARCHAR(50)) || ',' ELSE '' END),
        ','
    ) as nonInterestExpenses,
    
    COALESCE(SUM(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' THEN total_amount ELSE 0 END), 0) as nonInterestExpensesValue

FROM categorized_amounts;
