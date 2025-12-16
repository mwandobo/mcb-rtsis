select CURRENT_TIMESTAMP as reportingDate,
gte.FK_GLG_ACCOUNTACCO as accountNumber,
'BANK OF TANZANIA' as accountName,
'TIPS' as accountType,
 null as subAccountType,
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
CURRENT_TIMESTAMP as maturityDate,
0 as allowanceProbableLoss,
0 as botProvision

FROM
    GLI_TRX_EXTRACT AS gte
JOIN
    GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
JOIN CUSTOMER c
    ON c.CUST_ID = gte.CUST_ID
LEFT JOIN CURRENCY cu
    ON UPPER(TRIM(cu.SHORT_DESCR)) = UPPER(TRIM(gte.CURRENCY_SHORT_DES))
WHERE gl.EXTERNAL_GLACCOUNT='100028000'