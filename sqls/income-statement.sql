-- =============================================================================
-- INCOME STATEMENT REPORT FOR BOT API - FINAL WORKING VERSION
-- List fields format: [{"1": 5823000.00}, {"2": 120000.00}]
-- =============================================================================
-- USAGE: Update the 4 date values below (search for ← SET DATE)
--        All 4 must always be kept in sync.
-- =============================================================================

WITH categorized_amounts AS (
    SELECT
        lookup.CATEGORY,
        lookup.ITEM_CODE,
        SUM(COALESCE(gte.DC_AMOUNT, 0)) AS total_amount
    FROM GLI_TRX_EXTRACT gte
    INNER JOIN GLG_ACCOUNT gl
        ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    INNER JOIN INCOME_STATEMENT_GL_LOOKUP lookup
        ON gl.EXTERNAL_GLACCOUNT = lookup.GL_ACCOUNT
    WHERE gte.TRN_DATE >= DATE('2016-01-01')    -- ← SET DATE: period start
      AND gte.TRN_DATE <= CURRENT_DATE    -- ← SET DATE: period end
      AND gl.EXTERNAL_GLACCOUNT NOT LIKE '401%'
      AND gl.EXTERNAL_GLACCOUNT NOT LIKE '402%'
      AND gl.EXTERNAL_GLACCOUNT NOT LIKE '403%'
      AND gl.EXTERNAL_GLACCOUNT NOT IN ('705190001','705190002','705190003')
    GROUP BY lookup.CATEGORY, lookup.ITEM_CODE
),

pattern_accounts AS (
    SELECT
        'INTEREST_INCOME' AS CATEGORY,
        11                AS ITEM_CODE,
        SUM(COALESCE(gte.DC_AMOUNT, 0)) AS total_amount
    FROM GLI_TRX_EXTRACT gte
    INNER JOIN GLG_ACCOUNT gl
        ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    WHERE gte.TRN_DATE >= DATE('2016-01-01')    -- ← SET DATE: period start
      AND gte.TRN_DATE <= CURRENT_DATE    -- ← SET DATE: period end
      AND (   gl.EXTERNAL_GLACCOUNT LIKE '401%'
           OR gl.EXTERNAL_GLACCOUNT LIKE '402%'
           OR gl.EXTERNAL_GLACCOUNT LIKE '403%')
),

ecl_for_d49 AS (
    SELECT
        'NON_INTEREST_EXPENSE' AS CATEGORY,
        33                     AS ITEM_CODE,
        SUM(COALESCE(gte.DC_AMOUNT, 0)) AS total_amount
    FROM GLI_TRX_EXTRACT gte
    INNER JOIN GLG_ACCOUNT gl
        ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    WHERE gte.TRN_DATE >= DATE('2016-01-01')    -- ← SET DATE: period start
      AND gte.TRN_DATE <= CURRENT_DATE    -- ← SET DATE: period end
      AND gl.EXTERNAL_GLACCOUNT IN ('705190001','705190002','705190003')
),

provision_standalone AS (
    SELECT
        'PROVISION' AS CATEGORY,
        1           AS ITEM_CODE,
        SUM(COALESCE(gte.DC_AMOUNT, 0)) AS total_amount
    FROM GLI_TRX_EXTRACT gte
    INNER JOIN GLG_ACCOUNT gl
        ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    WHERE gte.TRN_DATE >= DATE('2016-01-01')    -- ← SET DATE: period start
      AND gte.TRN_DATE <= CURRENT_DATE    -- ← SET DATE: period end
      AND gl.EXTERNAL_GLACCOUNT IN ('705190001','705190002')
),

impairment_standalone AS (
    SELECT
        'IMPAIRMENT' AS CATEGORY,
        1            AS ITEM_CODE,
        SUM(COALESCE(gte.DC_AMOUNT, 0)) AS total_amount
    FROM GLI_TRX_EXTRACT gte
    INNER JOIN GLG_ACCOUNT gl
        ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    WHERE gte.TRN_DATE >= DATE('2016-01-01')    -- ← SET DATE: period start
      AND gte.TRN_DATE <= CURRENT_DATE   -- ← SET DATE: period end
      AND gl.EXTERNAL_GLACCOUNT IN ('705190003')
),

all_amounts AS (
    SELECT * FROM categorized_amounts
    UNION ALL
    SELECT * FROM pattern_accounts
    UNION ALL
    SELECT * FROM ecl_for_d49
    UNION ALL
    SELECT * FROM provision_standalone
    UNION ALL
    SELECT * FROM impairment_standalone
)

SELECT
    TO_CHAR(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI') AS reportingDate,

    -- -------------------------------------------------------------------------
    -- D46: Interest Income
    -- Format: [{"1": 5000000.00}, {"2": 200000.00}, ...]
    -- -------------------------------------------------------------------------
    '[' || RTRIM(
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 1  AND total_amount != 0
            THEN '{"1":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 2  AND total_amount != 0
            THEN '{"2":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 3  AND total_amount != 0
            THEN '{"3":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 4  AND total_amount != 0
            THEN '{"4":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 5  AND total_amount != 0
            THEN '{"5":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 6  AND total_amount != 0
            THEN '{"6":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 7  AND total_amount != 0
            THEN '{"7":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 8  AND total_amount != 0
            THEN '{"8":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 9  AND total_amount != 0
            THEN '{"9":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 10 AND total_amount != 0
            THEN '{"10":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_INCOME' AND ITEM_CODE = 11 AND total_amount != 0
            THEN '{"11":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), ''),
        ','
    ) || ']' AS interestIncome,

    COALESCE(SUM(CASE WHEN CATEGORY = 'INTEREST_INCOME'
                 THEN total_amount ELSE 0 END), 0) AS interestIncomeValue,

    -- -------------------------------------------------------------------------
    -- D47: Interest Expenses
    -- Format: [{"1": 3000000.00}, {"5": 100000.00}, ...]
    -- -------------------------------------------------------------------------
    '[' || RTRIM(
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 1  AND total_amount != 0
            THEN '{"1":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 2  AND total_amount != 0
            THEN '{"2":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 3  AND total_amount != 0
            THEN '{"3":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 4  AND total_amount != 0
            THEN '{"4":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 5  AND total_amount != 0
            THEN '{"5":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 6  AND total_amount != 0
            THEN '{"6":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 7  AND total_amount != 0
            THEN '{"7":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 8  AND total_amount != 0
            THEN '{"8":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 9  AND total_amount != 0
            THEN '{"9":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 10 AND total_amount != 0
            THEN '{"10":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'INTEREST_EXPENSE' AND ITEM_CODE = 11 AND total_amount != 0
            THEN '{"11":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), ''),
        ','
    ) || ']' AS interestExpenses,

    COALESCE(SUM(CASE WHEN CATEGORY = 'INTEREST_EXPENSE'
                 THEN total_amount ELSE 0 END), 0) AS interestExpensesValue,

    -- -------------------------------------------------------------------------
    -- Standalone provision / impairment fields (numeric only, no list format)
    -- -------------------------------------------------------------------------
    0 AS badDebtsWrittenOffNotProvided,

    COALESCE(SUM(CASE WHEN CATEGORY = 'PROVISION'
                 THEN total_amount ELSE 0 END), 0) AS provisionBadDoubtfulDebts,

    COALESCE(SUM(CASE WHEN CATEGORY = 'IMPAIRMENT'
                 THEN total_amount ELSE 0 END), 0) AS impairmentsInvestments,

    0 AS incomeTaxProvision,

    0 AS extraordinaryCreditsCharge,

    -- -------------------------------------------------------------------------
    -- D50: Non-Core Credits & Charges
    -- Format: [{"1": 50000.00}, {"3": 120000.00}]
    -- -------------------------------------------------------------------------
    '[' || RTRIM(
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_CORE_CREDITS' AND ITEM_CODE = 1 AND total_amount != 0
            THEN '{"1":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_CORE_CREDITS' AND ITEM_CODE = 2 AND total_amount != 0
            THEN '{"2":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_CORE_CREDITS' AND ITEM_CODE = 3 AND total_amount != 0
            THEN '{"3":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), ''),
        ','
    ) || ']' AS nonCoreCreditsCharges,

    COALESCE(SUM(CASE WHEN CATEGORY = 'NON_CORE_CREDITS'
                 THEN total_amount ELSE 0 END), 0) AS nonCoreCreditsChargesValue,

    -- -------------------------------------------------------------------------
    -- D48: Non-Interest Income
    -- Format: [{"1": 200000.00}, {"4": 500000.00}, ...]
    -- -------------------------------------------------------------------------
    '[' || RTRIM(
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 1  AND total_amount != 0
            THEN '{"1":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 2  AND total_amount != 0
            THEN '{"2":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 3  AND total_amount != 0
            THEN '{"3":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 4  AND total_amount != 0
            THEN '{"4":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 5  AND total_amount != 0
            THEN '{"5":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 6  AND total_amount != 0
            THEN '{"6":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 7  AND total_amount != 0
            THEN '{"7":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 8  AND total_amount != 0
            THEN '{"8":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 9  AND total_amount != 0
            THEN '{"9":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 14 AND total_amount != 0
            THEN '{"14":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 15 AND total_amount != 0
            THEN '{"15":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 17 AND total_amount != 0
            THEN '{"17":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 20 AND total_amount != 0
            THEN '{"20":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 24 AND total_amount != 0
            THEN '{"24":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME' AND ITEM_CODE = 28 AND total_amount != 0
            THEN '{"28":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), ''),
        ','
    ) || ']' AS nonInterestIncome,

    COALESCE(SUM(CASE WHEN CATEGORY = 'NON_INTEREST_INCOME'
                 THEN total_amount ELSE 0 END), 0) AS nonInterestIncomeValue,

    -- -------------------------------------------------------------------------
    -- D49: Non-Interest Expenses
    -- Format: [{"1": 800000.00}, {"2": 2000000.00}, ...]
    -- -------------------------------------------------------------------------
    '[' || RTRIM(
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 1  AND total_amount != 0
            THEN '{"1":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 2  AND total_amount != 0
            THEN '{"2":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 3  AND total_amount != 0
            THEN '{"3":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 5  AND total_amount != 0
            THEN '{"5":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 6  AND total_amount != 0
            THEN '{"6":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 7  AND total_amount != 0
            THEN '{"7":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 8  AND total_amount != 0
            THEN '{"8":'  || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 12 AND total_amount != 0
            THEN '{"12":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 13 AND total_amount != 0
            THEN '{"13":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 20 AND total_amount != 0
            THEN '{"20":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 21 AND total_amount != 0
            THEN '{"21":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 22 AND total_amount != 0
            THEN '{"22":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 23 AND total_amount != 0
            THEN '{"23":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 24 AND total_amount != 0
            THEN '{"24":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 29 AND total_amount != 0
            THEN '{"29":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 32 AND total_amount != 0
            THEN '{"32":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 33 AND total_amount != 0
            THEN '{"33":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 34 AND total_amount != 0
            THEN '{"34":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 35 AND total_amount != 0
            THEN '{"35":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 36 AND total_amount != 0
            THEN '{"36":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 38 AND total_amount != 0
            THEN '{"38":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 39 AND total_amount != 0
            THEN '{"39":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 40 AND total_amount != 0
            THEN '{"40":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 41 AND total_amount != 0
            THEN '{"41":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), '') ||
        COALESCE(MAX(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE' AND ITEM_CODE = 55 AND total_amount != 0
            THEN '{"55":' || CAST(ROUND(total_amount, 2) AS VARCHAR(50)) || '},' ELSE '' END), ''),
        ','
    ) || ']' AS nonInterestExpenses,

    COALESCE(SUM(CASE WHEN CATEGORY = 'NON_INTEREST_EXPENSE'
                 THEN total_amount ELSE 0 END), 0) AS nonInterestExpensesValue

FROM all_amounts;