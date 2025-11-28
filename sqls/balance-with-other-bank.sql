select 
    CURRENT_TIMESTAMP AS reportingDate,
    gte.FK0UNITCODE as bankCode,
    c.FIRST_NAME,
    'Tanzania' as Country,
    gte.CURRENCY_SHORT_DES as currency,
    -- orgAmount: always original DC_AMOUNT
    gte.DC_AMOUNT AS orgAmount,

    -- USD Amount: only if currency is USD, otherwise null
    CASE 
        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
            THEN gte.DC_AMOUNT
        ELSE NULL
    END AS usdAmount,

    -- TZS Amount: convert only if USD, otherwise use as is
    CASE 
        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
            THEN gte.DC_AMOUNT * 2500   -- <<< replace with your rate
        ELSE 
            gte.DC_AMOUNT
    END AS tzsAmount,
    gte.TRN_DATE as transactionDate,
    gte.AVAILABILITY_DATE as maturityDate,
     -- MaturityDate minus TransactionDate in days
    (DATE(CURRENT_TIMESTAMP) - DATE(gte.TRN_DATE)) AS pastDueDays
FROM
GLI_TRX_EXTRACT as gte

JOIN 
    GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
    
JOIN 
    CUSTOMER c ON gte.CUST_ID = c.CUST_ID
    
where gl.EXTERNAL_GLACCOUNT IN ('100013000','100050000','100050001');