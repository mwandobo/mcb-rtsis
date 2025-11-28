select CURRENT_TIMESTAMP as reportingDate,
gte.FK_GLG_ACCOUNTACCO as accountNumber,
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
LTRIM(RTRIM(COALESCE(c.first_name, '') || ' ' || COALESCE(c.middle_name, '') || ' ' || COALESCE(c.surname, ''))) AS accountName,
'TIPS' as accountType,
CURRENT_TIMESTAMP as maturityDate

FROM
    GLI_TRX_EXTRACT AS gte
JOIN
    GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
JOIN CUSTOMER c
    ON c.CUST_ID = gte.CUST_ID
--JOIN RATE_TABLE r
    --ON r.TMSTAMP = gte.TMSTAMP
WHERE gl.EXTERNAL_GLACCOUNT='100028000';