-- Query to extract interBranchFloatItem data from PROFITS database
-- This query combines inter-branch remittance data with branch and currency information

SELECT 
    -- Core inter-branch float item identifier
    ir.REMITTANCE_NUMBER as interBranchFloatItem,
    
    -- Reporting date (using the date issued or received)
    COALESCE(ir.DATE_RECEIVED, ir.DATE_ISSUED, CURRENT_DATE) as reportingDate,
    
    -- Branch information
    u1.UNIT_NAME as branchNamestring,
    ir.FK_UNITCODE as branchCode,
    
    -- Currency information
    c.SHORT_DESCR as currency,
    
    -- Amount information in original currency
    ir.AMOUNT as orgAmountFloat,
    
    -- Amount in TZS (assuming TZS currency ID, you may need to adjust)
    CASE 
        WHEN c.SHORT_DESCR = 'TZS' THEN ir.AMOUNT
        ELSE ir.AMOUNT * ir.RATE_TO_DC  -- Convert using rate to domestic currency
    END as tzsAmountFloat,
    
    -- Amount in USD (you may need to get USD exchange rate)
    CASE 
        WHEN c.SHORT_DESCR = 'USD' THEN ir.AMOUNT
        ELSE ir.AMOUNT / COALESCE(fx.USD_RATE, 1)  -- Convert to USD using exchange rate
    END as usdAmountFloat,
    
    -- Past due days calculation (days between issue and current date)
    CASE 
        WHEN ir.DATE_ISSUED IS NOT NULL 
        THEN (CURRENT_DATE - ir.DATE_ISSUED)
        ELSE 0
    END as pastDueDays,
    
    -- Allowance for probable loss (this may need to be calculated based on business rules)
    CASE 
        WHEN (CURRENT_DATE - COALESCE(ir.DATE_ISSUED, ir.DATE_RECEIVED)) > 90 
        THEN ir.AMOUNT_IN_DC * 0.50  -- 50% provision for items over 90 days
        WHEN (CURRENT_DATE - COALESCE(ir.DATE_ISSUED, ir.DATE_RECEIVED)) > 60 
        THEN ir.AMOUNT_IN_DC * 0.25  -- 25% provision for items over 60 days
        WHEN (CURRENT_DATE - COALESCE(ir.DATE_ISSUED, ir.DATE_RECEIVED)) > 30 
        THEN ir.AMOUNT_IN_DC * 0.10  -- 10% provision for items over 30 days
        ELSE 0
    END as allowanceProbableLoss,
    
    -- Classification based on aging
    CASE 
        WHEN (CURRENT_DATE - COALESCE(ir.DATE_ISSUED, ir.DATE_RECEIVED)) <= 30 
        THEN 'CURRENT'
        WHEN (CURRENT_DATE - COALESCE(ir.DATE_ISSUED, ir.DATE_RECEIVED)) <= 60 
        THEN 'SUBSTANDARD'
        WHEN (CURRENT_DATE - COALESCE(ir.DATE_ISSUED, ir.DATE_RECEIVED)) <= 90 
        THEN 'DOUBTFUL'
        ELSE 'LOSS'
    END as classification

FROM INTERBRANCH_REMITT ir
    
    -- Join with currency table to get currency description
    LEFT JOIN CURRENCY c ON ir.FK_CURRENCYID_CURR = c.ID_CURRENCY
    
    -- Join with unit table to get branch name (sending branch)
    LEFT JOIN UNIT u1 ON ir.FK_UNITCODE = u1.CODE
    
    -- Join with unit table to get receiving branch name
    LEFT JOIN UNIT u2 ON ir.FK0UNITCODE = u2.CODE
    
    -- Optional: Join with fixing rate or currency table to get USD exchange rates
    LEFT JOIN (
        SELECT 
            FK_CURRENCYID_CURR,
            RATE as USD_RATE,
            ROW_NUMBER() OVER (PARTITION BY FK_CURRENCYID_CURR ORDER BY TMSTAMP DESC) as rn
        FROM FIXING_RATE 
        WHERE FK_CURRENCYID_CURR IN (
            SELECT ID_CURRENCY FROM CURRENCY WHERE SHORT_DESCR = 'USD'
        )
    ) fx ON c.ID_CURRENCY = fx.FK_CURRENCYID_CURR AND fx.rn = 1

WHERE 
    -- Filter for active/pending inter-branch items
    ir.ENTRY_STATUS IN ('1', 'P', 'A')  -- Adjust status codes as per your business rules
    
    -- Optional: Filter by date range
    AND ir.DATE_ISSUED >= (CURRENT_DATE - 365)  -- Last 365 days
    
ORDER BY 
    ir.DATE_ISSUED DESC,
    ir.REMITTANCE_NUMBER;

-- Alternative query if you need to include denomination details
-- This would be useful if you want to break down float items by denomination

/*
SELECT 
    ir.REMITTANCE_NUMBER as interBranchFloatItem,
    COALESCE(ir.DATE_RECEIVED, ir.DATE_ISSUED, CURRENT_DATE) as reportingDate,
    u1.UNIT_NAME as branchNamestring,
    ir.FK_UNITCODE as branchCode,
    c.SHORT_DESCR as currency,
    
    -- Denomination specific amounts
    (id.QUANTITY * id.DENOMINATION) as orgAmountFloat,
    
    CASE 
        WHEN c.SHORT_DESCR = 'TZS' THEN (id.QUANTITY * id.DENOMINATION)
        ELSE (id.QUANTITY * id.DENOMINATION) * ir.RATE_TO_DC
    END as tzsAmountFloat,
    
    CASE 
        WHEN c.SHORT_DESCR = 'USD' THEN (id.QUANTITY * id.DENOMINATION)
        ELSE (id.QUANTITY * id.DENOMINATION) / COALESCE(fx.USD_RATE, 1)
    END as usdAmountFloat,
    
    CASE 
        WHEN ir.DATE_ISSUED IS NOT NULL 
        THEN (CURRENT_DATE - ir.DATE_ISSUED)
        ELSE 0
    END as pastDueDays,
    
    -- Allowance calculation
    CASE 
        WHEN (CURRENT_DATE - COALESCE(ir.DATE_ISSUED, ir.DATE_RECEIVED)) > 90 
        THEN (id.QUANTITY * id.DENOMINATION) * ir.RATE_TO_DC * 0.50
        WHEN (CURRENT_DATE - COALESCE(ir.DATE_ISSUED, ir.DATE_RECEIVED)) > 60 
        THEN (id.QUANTITY * id.DENOMINATION) * ir.RATE_TO_DC * 0.25
        WHEN (CURRENT_DATE - COALESCE(ir.DATE_ISSUED, ir.DATE_RECEIVED)) > 30 
        THEN (id.QUANTITY * id.DENOMINATION) * ir.RATE_TO_DC * 0.10
        ELSE 0
    END as allowanceProbableLoss,
    
    CASE 
        WHEN (CURRENT_DATE - COALESCE(ir.DATE_ISSUED, ir.DATE_RECEIVED)) <= 30 
        THEN 'CURRENT'
        WHEN (CURRENT_DATE - COALESCE(ir.DATE_ISSUED, ir.DATE_RECEIVED)) <= 60 
        THEN 'SUBSTANDARD'
        WHEN (CURRENT_DATE - COALESCE(ir.DATE_ISSUED, ir.DATE_RECEIVED)) <= 90 
        THEN 'DOUBTFUL'
        ELSE 'LOSS'
    END as classification,
    
    -- Additional denomination details
    id.DENOMINATION,
    id.QUANTITY,
    id.NOTES_COINS_TYPE

FROM INTERBRANCH_REMITT ir
    INNER JOIN IBRANCH_DENOMINATI id ON ir.REMITTANCE_NUMBER = id.FK_INTERBRANCH_REM
    LEFT JOIN CURRENCY c ON ir.FK_CURRENCYID_CURR = c.ID_CURRENCY
    LEFT JOIN UNIT u1 ON ir.FK_UNITCODE = u1.CODE
    LEFT JOIN UNIT u2 ON ir.FK0UNITCODE = u2.CODE
    LEFT JOIN (
        SELECT 
            FK_CURRENCYID_CURR,
            RATE as USD_RATE,
            ROW_NUMBER() OVER (PARTITION BY FK_CURRENCYID_CURR ORDER BY TMSTAMP DESC) as rn
        FROM FIXING_RATE 
        WHERE FK_CURRENCYID_CURR IN (
            SELECT ID_CURRENCY FROM CURRENCY WHERE SHORT_DESCR = 'USD'
        )
    ) fx ON c.ID_CURRENCY = fx.FK_CURRENCYID_CURR AND fx.rn = 1

WHERE 
    ir.ENTRY_STATUS IN ('1', 'P', 'A')
    AND ir.DATE_ISSUED >= (CURRENT_DATE - 365)

ORDER BY 
    ir.DATE_ISSUED DESC,
    ir.REMITTANCE_NUMBER,
    id.SERIAL_NUM;
*/