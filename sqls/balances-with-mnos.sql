SELECT
    CURRENT_TIMESTAMP AS reportingDate,
    CURRENT_TIMESTAMP AS floatBalanceDate,

    -- mnoCode based on EXTERNAL_GLACCOUNT
    CASE gl.EXTERNAL_GLACCOUNT
        WHEN '504080001' THEN 'Jumo'
        WHEN '144000051' THEN 'Airtel Money'
        WHEN '144000058' THEN 'Tigo Pesa'
        WHEN '144000061' THEN 'Halopesa'
        WHEN '144000062' THEN 'M-Pesa'
        ELSE 'None'
    END AS mnoCode,

    pa.ACCOUNT_NUMBER AS tillNumber,
    gte.CURRENCY_SHORT_DES AS currency,
    0 AS allowanceProbableLoss,
    0 AS botProvision,

    -- orgAmount: always original DC_AMOUNT
    gte.DC_AMOUNT AS orgFloatAmount,

    -- USD Amount: only if currency is USD, otherwise null
    CASE
         WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
           WHEN gte.CURRENCY_SHORT_DES IN ('TZ', 'TZS')
               THEN DECIMAL(gte.DC_AMOUNT / 2500.00, 18, 2) -- <<< TZS → USD
           ELSE
               NULL
    END AS usdFloatAmount,

    -- TZS Amount: convert only if USD, otherwise use as is
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT * 2500.00, 18, 2)
           WHEN gte.CURRENCY_SHORT_DES IN ('TZ', 'TZS')
               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
           ELSE
               NULL
    END AS tzsFloatAmount

FROM GLI_TRX_EXTRACT gte
JOIN GLG_ACCOUNT gl
    ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = gte.CUST_ID
WHERE gl.EXTERNAL_GLACCOUNT IN (
    '504080001','144000051','144000058','144000061','144000062'
);
