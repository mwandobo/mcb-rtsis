



-- =============================================================================
-- SECTION 4: MAIN QUERY
-- =============================================================================
-- USAGE: Replace the date values in the two WHERE clauses below.
--        Both must always be kept in sync.
--        Format: DATE('YYYY-MM-DD')
--
-- KNOWN GAPS (confirm with finance before go-live):
--   - D46 codes 4,5,6,7,9: No GLs for BoT/Govt/Microfinance interest income
--   - D47 codes 2,3,4,6,7,9,10: No GLs for Govt/BoT/special deposit interest expense
--   - D47 code 11 (601070001): May belong to code 3 — verify with finance
--   - D48 code 2: No GL for foreign operations commission
--   - D49 code 40: NSSF GL not yet identified
--   - D50 code 2: No GL for income from assets acquired
--   - incomeTaxProvision: No GL identified — confirm TRA payment GL
--   - extraordinaryCreditsCharge: No GL identified — confirm with finance
--   - FX Losses (505040002): Confirm sign convention in DC_AMOUNT
-- =============================================================================

WITH categorized_amounts AS (
    -- Single scan with lookup join
    -- Excludes 401/402/403 accounts to prevent double-counting with pattern_accounts CTE
    SELECT 
        lookup.CATEGORY,
        lookup.ITEM_CODE,
        SUM(COALESCE(gte.DC_AMOUNT, 0)) AS total_amount
    FROM GLI_TRX_EXTRACT gte
    INNER JOIN GLG_ACCOUNT gl 
        ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    INNER JOIN INCOME_STATEMENT_GL_LOOKUP lookup 
        ON gl.EXTERNAL_GLACCOUNT = lookup.GL_ACCOUNT
    WHERE gte.TRN_DATE >= DATE('2025-01-01')   -- ← SET PERIOD START DATE
      AND gte.TRN_DATE <  DATE('2026-01-01')   -- ← SET PERIOD END DATE
      -- Exclude pattern-matched accounts to prevent double-counting
      AND gl.EXTERNAL_GLACCOUNT NOT LIKE '401%'
      AND gl.EXTERNAL_GLACCOUNT NOT LIKE '402%'
      AND gl.EXTERNAL_GLACCOUNT NOT LIKE '403%'
    GROUP BY lookup.CATEGORY, lookup.ITEM_CODE
),

pattern_accounts AS (
    -- Handles NPL and Arrears interest accounts (401%, 402%, 403%)
    -- Mapped to INTEREST_INCOME Code 11: Interest Recovery from Provisioned Assets
    -- Only runs if these accounts are NOT already in the lookup table
    -- If you added 401/402/403 GLs explicitly to the lookup above, comment out this CTE
    SELECT 
        'INTEREST_INCOME'   AS CATEGORY,
        11                  AS ITEM_CODE,
        SUM(COALESCE(gte.DC_AMOUNT, 0)) AS total_amount
    FROM GLI_TRX_EXTRACT gte
    INNER JOIN GLG_ACCOUNT gl 
        ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    WHERE gte.TRN_DATE >= DATE('2025-01-01')   -- ← KEEP IN SYNC WITH ABOVE
      AND gte.TRN_DATE <  DATE('2026-01-01')   -- ← KEEP IN SYNC WITH ABOVE
      AND (   gl.EXTERNAL_GLACCOUNT LIKE '401%'
           OR gl.EXTERNAL_GLACCOUNT LIKE '402%'
           OR gl.EXTERNAL_GLACCOUNT LIKE '403%')
),

all_amounts AS (
    SELECT * FROM categorized_amounts
    UNION ALL
    SELECT * FROM pattern_accounts
)

SELECT 
    -- Reporting timestamp
    TO_CHAR(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI') AS reportingDate,

    -- -------------------------------------------------------------------------
    -- D46: Interest Income
    -- Produces string like: "1:5000000,2:200000,3:150000,8:300000,11:75000"
    -- -------------------------------------------------------------------------
    RTRIM(
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 1  AND total_amount != 0
            THEN '1:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 2  AND total_amount != 0
            THEN '2:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 3  AND total_amount != 0
            THEN '3:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 4  AND total_amount != 0
            THEN '4:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 5  AND total_amount != 0
            THEN '5:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 6  AND total_amount != 0
            THEN '6:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 7  AND total_amount != 0
            THEN '7:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 8  AND total_amount != 0
            THEN '8:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 9  AND total_amount != 0
            THEN '9:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 10 AND total_amount != 0
            THEN '10:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 11 AND total_amount != 0
            THEN '11:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), ''),
        ','
    ) AS interestIncome,

    COALESCE(SUM(CASE WHEN CATEGORY = 'INTEREST_INCOME' THEN total_amount ELSE 0 END), 0) 
        AS interestIncomeValue,

    -- -------------------------------------------------------------------------
    -- D47: Interest Expenses
    -- Produces string like: "1:3000000,5:100000,8:50000,11:25000"
    -- -------------------------------------------------------------------------
    RTRIM(
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 1  AND total_amount != 0
            THEN '1:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 2  AND total_amount != 0
            THEN '2:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 3  AND total_amount != 0
            THEN '3:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 4  AND total_amount != 0
            THEN '4:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 5  AND total_amount != 0
            THEN '5:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 6  AND total_amount != 0
            THEN '6:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 7  AND total_amount != 0
            THEN '7:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 8  AND total_amount != 0
            THEN '8:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 9  AND total_amount != 0
            THEN '9:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 10 AND total_amount != 0
            THEN '10:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 11 AND total_amount != 0
            THEN '11:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), ''),
        ','
    ) AS interestExpenses,

    COALESCE(SUM(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' THEN total_amount ELSE 0 END), 0) 
        AS interestExpensesValue,

    -- -------------------------------------------------------------------------
    -- Standalone fields
    -- NOTE: provisionBadDoubtfulDebts uses 705190001 + 705190002
    --       impairmentsInvestments uses 705190003
    --       These GLs are ALSO included in NON_INTEREST_EXPENSE code 33 (ECL).
    --       Confirm with BOT whether standalone fields should be populated
    --       in addition to, or instead of, the D49 code 33 entry.
    -- -------------------------------------------------------------------------
    COALESCE(SUM(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 33
                       AND GL_ACCOUNT IN ('705190001','705190002')
                  THEN total_amount ELSE 0 END), 0) 
        AS badDebtsWrittenOffNotProvided,
        -- TODO: Identify specific write-off GL separate from provision GL

    COALESCE(SUM(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 33
                       AND GL_ACCOUNT IN ('705190001','705190002')
                  THEN total_amount ELSE 0 END), 0) 
        AS provisionBadDoubtfulDebts,

    COALESCE(SUM(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 33
                       AND GL_ACCOUNT IN ('705190003')
                  THEN total_amount ELSE 0 END), 0) 
        AS impairmentsInvestments,

    -- TODO: Identify income tax GL (quarterly TRA payment) and add to lookup
    0 AS incomeTaxProvision,

    -- TODO: Identify extraordinary/recurring unusual items GL and add to lookup
    0 AS extraordinaryCreditsCharge,

    -- -------------------------------------------------------------------------
    -- D50: Non-Core Credits & Charges
    -- -------------------------------------------------------------------------
    RTRIM(
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_CORE_CREDITS' AND ITEM_CODE = 1 AND total_amount != 0
            THEN '1:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_CORE_CREDITS' AND ITEM_CODE = 2 AND total_amount != 0
            THEN '2:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_CORE_CREDITS' AND ITEM_CODE = 3 AND total_amount != 0
            THEN '3:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), ''),
        ','
    ) AS nonCoreCreditsCharges,

    COALESCE(SUM(CASE WHEN CATEGORY = 'NON_CORE_CREDITS' THEN total_amount ELSE 0 END), 0) 
        AS nonCoreCreditsChargesValue,

    -- -------------------------------------------------------------------------
    -- D48: Non-Interest Income
    -- Produces string like: "1:200000,4:500000,5:300000,20:50000,24:75000,28:400000"
    -- -------------------------------------------------------------------------
    RTRIM(
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 1  AND total_amount != 0
            THEN '1:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 2  AND total_amount != 0
            THEN '2:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 3  AND total_amount != 0
            THEN '3:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 4  AND total_amount != 0
            THEN '4:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 5  AND total_amount != 0
            THEN '5:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 6  AND total_amount != 0
            THEN '6:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 7  AND total_amount != 0
            THEN '7:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 8  AND total_amount != 0
            THEN '8:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 9  AND total_amount != 0
            THEN '9:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 14 AND total_amount != 0
            THEN '14:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 15 AND total_amount != 0
            THEN '15:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 17 AND total_amount != 0
            THEN '17:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 20 AND total_amount != 0
            THEN '20:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 24 AND total_amount != 0
            THEN '24:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 28 AND total_amount != 0
            THEN '28:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), ''),
        ','
    ) AS nonInterestIncome,

    COALESCE(SUM(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' THEN total_amount ELSE 0 END), 0) 
        AS nonInterestIncomeValue,

    -- -------------------------------------------------------------------------
    -- D49: Non-Interest Expenses
    -- -------------------------------------------------------------------------
    RTRIM(
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 1  AND total_amount != 0
            THEN '1:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 2  AND total_amount != 0
            THEN '2:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 3  AND total_amount != 0
            THEN '3:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 5  AND total_amount != 0
            THEN '5:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 6  AND total_amount != 0
            THEN '6:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 7  AND total_amount != 0
            THEN '7:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 8  AND total_amount != 0
            THEN '8:'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 12 AND total_amount != 0
            THEN '12:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 13 AND total_amount != 0
            THEN '13:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 20 AND total_amount != 0
            THEN '20:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 21 AND total_amount != 0
            THEN '21:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 22 AND total_amount != 0
            THEN '22:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 23 AND total_amount != 0
            THEN '23:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 24 AND total_amount != 0
            THEN '24:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 29 AND total_amount != 0
            THEN '29:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 32 AND total_amount != 0
            THEN '32:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 33 AND total_amount != 0
            THEN '33:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 34 AND total_amount != 0
            THEN '34:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 35 AND total_amount != 0
            THEN '35:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 36 AND total_amount != 0
            THEN '36:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 38 AND total_amount != 0
            THEN '38:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 39 AND total_amount != 0
            THEN '39:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 40 AND total_amount != 0
            THEN '40:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 41 AND total_amount != 0
            THEN '41:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 55 AND total_amount != 0
            THEN '55:' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || ',' ELSE '' END), ''),
        ','
    ) AS nonInterestExpenses,

    COALESCE(SUM(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' THEN total_amount ELSE 0 END), 0) 
        AS nonInterestExpensesValue

FROM all_amounts;


-- =============================================================================
-- SECTION 5: VALIDATION QUERY
-- Run this after the main query to spot unmapped GLs
-- =============================================================================

-- Shows all GL accounts with transactions in the period that are NOT in the lookup table
-- Any rows here represent money that is silently excluded from the report
SELECT DISTINCT
    gl.EXTERNAL_GLACCOUNT,
    gl.ACCOUNT_NAME,
    SUM(COALESCE(gte.DC_AMOUNT, 0)) AS total_unmatched_amount
FROM GLI_TRX_EXTRACT gte
INNER JOIN GLG_ACCOUNT gl 
    ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
LEFT JOIN INCOME_STATEMENT_GL_LOOKUP lookup 
    ON gl.EXTERNAL_GLACCOUNT = lookup.GL_ACCOUNT
WHERE gte.TRN_DATE >= DATE('2025-01-01')   -- ← MATCH MAIN QUERY DATES
  AND gte.TRN_DATE <  DATE('2026-01-01')   -- ← MATCH MAIN QUERY DATES
  AND lookup.GL_ACCOUNT IS NULL
  -- Only flag income statement relevant GL ranges
  AND (   gl.EXTERNAL_GLACCOUNT LIKE '4%'
       OR gl.EXTERNAL_GLACCOUNT LIKE '5%'
       OR gl.EXTERNAL_GLACCOUNT LIKE '6%'
       OR gl.EXTERNAL_GLACCOUNT LIKE '7%')
GROUP BY gl.EXTERNAL_GLACCOUNT, gl.ACCOUNT_NAME
HAVING SUM(COALESCE(gte.DC_AMOUNT, 0)) != 0
ORDER BY gl.EXTERNAL_GLACCOUNT;