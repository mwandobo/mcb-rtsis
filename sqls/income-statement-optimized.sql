-- Income Statement Report for BOT API (Optimized Version)
-- Performance improvements:
-- 1. Single pass through data with CASE expressions
-- 2. Conditional aggregation instead of multiple SUMs
-- 3. Date filtering for current period only
-- 4. Efficient string building

WITH income_data AS (
    -- Single scan of transaction data with date filter
    SELECT 
        gl.EXTERNAL_GLACCOUNT,
        SUM(COALESCE(gte.DC_AMOUNT, 0)) as amount
    FROM GLI_TRX_EXTRACT gte
    INNER JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    WHERE 
        -- Filter by current month/period for performance
        gte.TRX_DATE >= CURRENT_DATE - 30 DAYS
        AND gl.EXTERNAL_GLACCOUNT IN (
            -- Interest Income accounts (D46)
            '400010001','400010005','400020001','400020012','400030001','400030002','400030003','400030005',
            '400040001','400040002','400040003','400040006','400060001','400060002','400060004','400060005',
            '400061001','400070003','404020001','408010001','400030007','400030009',
            -- Interest Expense accounts (D47)
            '600220001','600220002','600220003','600220004','600220005','600220006','600220009','600220010',
            '600230001','600230002','600230003','600230004','600230005','600230006','600240001','601060001',
            '602010001',
            -- Non-Interest Income accounts (D48)
            '500010009','500010010','500010015','500020001','500020003','500020010','500020011','500020014',
            '500020015','500020016','500020017','500020018','500020019','500020020','500020027','500020030',
            '500020036','500020038','500020055','500020056','500020058','500020062','500020065','500020070',
            '502010001','503010001','503010011','503010012','504040001','504040002','504050001','504060001',
            '504080001','504100001','504110001','504120002','504130001','505000000','505010001','505040001',
            '505040002','505040006','505040009','505070001',
            -- Non-Interest Expense accounts (D49)
            '700010001','700010004','700010005','700020002','700030001','700040002','700050001','700060001',
            '700090001','700090003','700090004','700090005','700090006','700090009','700090011','700090013',
            '700090015','701010001','701010002','701010003','701010006','701010008','701010009','701010010',
            '701010012','701010013','701020001','701020005','701020006','702010002','702030001','702030003',
            '702030004','702040002','702040004','702040006','703020001','703020004','703020006','703020008',
            '703020011','704010001','704020001','704020002','704020004','704020007','704020012','704020013',
            '704030004','705010001','705010002','705010003','705020001','705040001','705040009','705040011',
            '705050001','705060002','705060004','705070001','705080002','705090001','705100003','705100004',
            '705110002','705110003','705110004','705110005','705110006','705110007','705120001','705120005',
            '705120006','705160001','705160002','705160003','705170001','705190001','705190002','705190003'
        )
        -- Add pattern matching for 401%, 402%, 403% accounts
        OR (gl.EXTERNAL_GLACCOUNT LIKE '401%' OR gl.EXTERNAL_GLACCOUNT LIKE '402%' OR gl.EXTERNAL_GLACCOUNT LIKE '403%')
    GROUP BY gl.EXTERNAL_GLACCOUNT
),
categorized_amounts AS (
    -- Categorize amounts by item codes in a single pass
    SELECT
        -- D46: Interest Income categories
        CASE 
            WHEN EXTERNAL_GLACCOUNT IN ('400010001','400010005','400020001','400020012','400030001','400030002','400030003','400030005','400040001','400040002','400040003','400040006','400030007','400030009') THEN 1
            WHEN EXTERNAL_GLACCOUNT = '408010001' THEN 2
            WHEN EXTERNAL_GLACCOUNT = '404020001' THEN 3
            WHEN EXTERNAL_GLACCOUNT IN ('400060001','400060002','400060005','400060004','400061001','400070003') THEN 6
            WHEN EXTERNAL_GLACCOUNT LIKE '401%' OR EXTERNAL_GLACCOUNT LIKE '402%' OR EXTERNAL_GLACCOUNT LIKE '403%' THEN 11
        END as interest_income_code,
        
        -- D47: Interest Expenses categories
        CASE 
            WHEN EXTERNAL_GLACCOUNT IN ('600220001','600220002','600220003','600220004','600220005','600220006','600220009','600220010') THEN 1
            WHEN EXTERNAL_GLACCOUNT = '602010001' THEN 5
            WHEN EXTERNAL_GLACCOUNT IN ('600230001','600230002','600230003','600230004','600230005','600230006','600240001','601060001') THEN 11
        END as interest_expense_code,
        
        -- D48: Non-Interest Income categories
        CASE 
            WHEN EXTERNAL_GLACCOUNT IN ('505040001','505040002','505040006','505040009') THEN 1
            WHEN EXTERNAL_GLACCOUNT IN ('500020055','500020056','500020058') THEN 4
            WHEN EXTERNAL_GLACCOUNT IN ('500010009','500010010','500010015','500020001','500020003','500020010','500020011','500020014','500020015','500020016','500020018','500020019','500020020','500020027','500020030','500020036','500020038','500020062','500020065','500020070','502010001','503010001','503010011','503010012','504040001','504040002','504050001','504060001','504080001','504100001','504110001','504120002','504130001') THEN 5
            WHEN EXTERNAL_GLACCOUNT = '500020017' THEN 20
        END as non_interest_income_code,
        
        -- D49: Non-Interest Expenses categories
        CASE 
            WHEN EXTERNAL_GLACCOUNT IN ('700010001','700010004','700010005','700020002','700030001','700040002','700050001','700060001','700090001','700090005','700090009','700090006','700090011','700090013','700090015','701020001','701020005','701020006') THEN 1
            WHEN EXTERNAL_GLACCOUNT = '701010001' THEN 5
            WHEN EXTERNAL_GLACCOUNT = '703020001' THEN 7
            WHEN EXTERNAL_GLACCOUNT IN ('703020004','703020006','703020008','703020011') THEN 8
            WHEN EXTERNAL_GLACCOUNT = '701010009' THEN 9
            WHEN EXTERNAL_GLACCOUNT = '705020001' THEN 12
            WHEN EXTERNAL_GLACCOUNT IN ('705070001','705100004','705110003','705100003','705110002','705110004','705110005','705110006','705110007') THEN 13
            WHEN EXTERNAL_GLACCOUNT = '705080002' THEN 23
            WHEN EXTERNAL_GLACCOUNT IN ('705040001','705040009','705040011') THEN 24
            WHEN EXTERNAL_GLACCOUNT IN ('700090003','700090004','701010002','701010003','701010006','701010008','701010012','701010013','701010010','702010002','702030001','702030003','702030004','702040002','702040004','702040006') THEN 25
            WHEN EXTERNAL_GLACCOUNT IN ('705050001','705060002','705060004','705090001') THEN 36
            WHEN EXTERNAL_GLACCOUNT IN ('705010001','705010002','705010003') THEN 37
            WHEN EXTERNAL_GLACCOUNT IN ('705120001','705120005','705120006') THEN 39
            WHEN EXTERNAL_GLACCOUNT IN ('705160001','705160002','705160003','705170001') THEN 40
        END as non_interest_expense_code,
        
        -- D50: Non-Core Credits and Charges categories
        CASE 
            WHEN EXTERNAL_GLACCOUNT = '505070001' THEN 1
            WHEN EXTERNAL_GLACCOUNT = '505010001' THEN 3
        END as non_core_code,
        
        -- Single value field indicators
        CASE WHEN EXTERNAL_GLACCOUNT = '705190003' THEN 'bad_debts' END as single_field_type,
        CASE WHEN EXTERNAL_GLACCOUNT = '705190002' THEN 'provision' END as provision_type,
        CASE WHEN EXTERNAL_GLACCOUNT = '705190001' THEN 'impairment' END as impairment_type,
        CASE WHEN EXTERNAL_GLACCOUNT IN ('704010001','704020001','704020002','704020004','704020007','704020012','704020013','704030004') THEN 'tax' END as tax_type,
        CASE WHEN EXTERNAL_GLACCOUNT = '505000000' THEN 'extraordinary' END as extraordinary_type,
        
        amount
    FROM income_data
)
SELECT 
    -- Reporting Date
    TO_CHAR(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI') as reportingDate,
    
    -- D46: Interest Income - Build string efficiently
    LISTAGG(interest_income_code || ':' || CAST(amount AS VARCHAR(50)), ',') 
        WITHIN GROUP (ORDER BY interest_income_code) 
        FILTER (WHERE interest_income_code IS NOT NULL AND amount > 0) as interestIncome,
    SUM(CASE WHEN interest_income_code IS NOT NULL THEN amount ELSE 0 END) as interestIncomeValue,
    
    -- D47: Interest Expenses
    LISTAGG(interest_expense_code || ':' || CAST(amount AS VARCHAR(50)), ',') 
        WITHIN GROUP (ORDER BY interest_expense_code) 
        FILTER (WHERE interest_expense_code IS NOT NULL AND amount > 0) as interestExpenses,
    SUM(CASE WHEN interest_expense_code IS NOT NULL THEN amount ELSE 0 END) as interestExpensesValue,
    
    -- Single value fields
    COALESCE(SUM(CASE WHEN single_field_type = 'bad_debts' THEN amount END), 0) as badDebtsWrittenOffNotProvided,
    COALESCE(SUM(CASE WHEN provision_type = 'provision' THEN amount END), 0) as provisionBadDoubtfulDebts,
    COALESCE(SUM(CASE WHEN impairment_type = 'impairment' THEN amount END), 0) as impairmentsInvestments,
    COALESCE(SUM(CASE WHEN tax_type = 'tax' THEN amount END), 0) as incomeTaxProvision,
    COALESCE(SUM(CASE WHEN extraordinary_type = 'extraordinary' THEN amount END), 0) as extraordinaryCreditsCharge,
    
    -- D50: Non-Core Credits and Charges
    LISTAGG(non_core_code || ':' || CAST(amount AS VARCHAR(50)), ',') 
        WITHIN GROUP (ORDER BY non_core_code) 
        FILTER (WHERE non_core_code IS NOT NULL AND amount > 0) as nonCoreCreditsCharges,
    SUM(CASE WHEN non_core_code IS NOT NULL THEN amount ELSE 0 END) as nonCoreCreditsChargesValue,
    
    -- D48: Non-Interest Income
    LISTAGG(non_interest_income_code || ':' || CAST(amount AS VARCHAR(50)), ',') 
        WITHIN GROUP (ORDER BY non_interest_income_code) 
        FILTER (WHERE non_interest_income_code IS NOT NULL AND amount > 0) as nonInterestIncome,
    SUM(CASE WHEN non_interest_income_code IS NOT NULL THEN amount ELSE 0 END) as nonInterestIncomeValue,
    
    -- D49: Non-Interest Expenses
    LISTAGG(non_interest_expense_code || ':' || CAST(amount AS VARCHAR(50)), ',') 
        WITHIN GROUP (ORDER BY non_interest_expense_code) 
        FILTER (WHERE non_interest_expense_code IS NOT NULL AND amount > 0) as nonInterestExpenses,
    SUM(CASE WHEN non_interest_expense_code IS NOT NULL THEN amount ELSE 0 END) as nonInterestExpensesValue

FROM categorized_amounts;
