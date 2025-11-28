SELECT
    CURRENT_TIMESTAMP,
    gte.CURRENCY_SHORT_DES as currency,
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
FROM GLI_TRX_EXTRACT as gte

JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID

WHERE gl.EXTERNAL_GLACCOUNT in ('504080001','144000051','144000058','144000061','144000062');