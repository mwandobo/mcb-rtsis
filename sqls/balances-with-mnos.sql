SELECT
    CURRENT_TIMESTAMP AS reportingDate,
    CURRENT_TIMESTAMP AS floatBalanceDate,

    -- mnoCode based on EXTERNAL_GLACCOUNT
    CASE gl.EXTERNAL_GLACCOUNT
        WHEN '504080001' THEN 'Super Agent Commission'
        WHEN '144000051' THEN 'AIRTEL Money Super Agent Float'
        WHEN '144000058' THEN 'TIGO PESA Super Agent Float'
        WHEN '144000061' THEN 'HALOPESA Super Agent Float'
        WHEN '144000062' THEN 'MPESA Super Agent Float'
        ELSE ''
    END AS mnoCode,

    gte.FK_GLG_ACCOUNTACCO AS tillNumber,
    gte.CURRENCY_SHORT_DES AS currency,
    0 AS allowanceProbableLoss,
    0 AS botProvision,

    -- orgAmount: always original DC_AMOUNT
    gte.DC_AMOUNT AS orgFloatAmount,

    -- USD Amount: only if currency is USD, otherwise null
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'USD'
            THEN gte.DC_AMOUNT
        ELSE NULL
    END AS usdFloatAmount,

    -- TZS Amount: convert only if USD, otherwise use as is
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'USD'
            THEN gte.DC_AMOUNT * 2500   -- <<< replace with your rate
        ELSE
            gte.DC_AMOUNT
    END AS tzsFloatAmount

FROM GLI_TRX_EXTRACT gte
JOIN GLG_ACCOUNT gl
    ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
WHERE gl.EXTERNAL_GLACCOUNT IN (
    '504080001','144000051','144000058','144000061','144000062'
);
