-- Income Statement Report for BOT API (Optimized with Lookup Table)
-- Uses INCOME_STATEMENT_GL_LOOKUP table for fast GL account categorization
-- Add date filter in WHERE clause for better performance
-- Example: WHERE gte.TRN_DATE >= '2024-01-01' AND gte.TRN_DATE < '2025-01-01'

WITH categorized_amounts AS (
    -- Single scan with lookup join - much faster than repeated CASE statements
    SELECT 
        lookup.CATEGORY,
        lookup.ITEM_CODE,
        SUM(COALESCE(gte.DC_AMOUNT, 0)) as total_amount
    FROM GLI_TRX_EXTRACT gte
    INNER JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    INNER JOIN INCOME_STATEMENT_GL_LOOKUP lookup ON gl.EXTERNAL_GLACCOUNT = lookup.GL_ACCOUNT
    WHERE gte.TRN_DATE >= DATE('2024-01-01')  -- Filter for current year - adjust as needed
    GROUP BY lookup.CATEGORY, lookup.ITEM_CODE
),
pattern_accounts AS (
    -- Handle pattern-based accounts (401%, 402%, 403%) separately
    SELECT 
        'INTEREST_INCOME' as CATEGORY,
        11 as ITEM_CODE,
        SUM(COALESCE(gte.DC_AMOUNT, 0)) as total_amount
    FROM GLI_TRX_EXTRACT gte
    INNER JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    WHERE gte.TRN_DATE >= DATE('2024-01-01')  -- Filter for current year - adjust as needed
        AND (gl.EXTERNAL_GLACCOUNT LIKE '401%' 
             OR gl.EXTERNAL_GLACCOUNT LIKE '402%' 
             OR gl.EXTERNAL_GLACCOUNT LIKE '403%')
),
all_amounts AS (
    -- Combine lookup-based and pattern-based amounts
    SELECT * FROM categorized_amounts
    UNION ALL
    SELECT * FROM pattern_accounts
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

FROM all_amounts;
