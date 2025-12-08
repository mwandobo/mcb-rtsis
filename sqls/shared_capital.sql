SELECT
    CURRENT_TIMESTAMP as reportingDate,
    '' as capitalSubCategory,
    gte.TRN_DATE as transactionDate,
    '' as transactionType,
    '' as shareholderNames,
    '' as clientType,
    '' as shareholderCountry,
    '' as numberOfShares,
    '' as sharePriceBookValue,
    gte.CURRENCY_SHORT_DES as currency,
    -- orgAmount: always original DC_AMOUNT
    gte.DC_AMOUNT AS orgAmount,
    -- USD Amount: only if currency is USD, otherwise null
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
        ELSE NULL
    END AS usdAmount,
    -- TZS Amount: convert only if USD, otherwise use as is
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT * 2500 -- <<< replace with your rate
        ELSE gte.DC_AMOUNT
    END AS tzsAmount,
    '' as sectorSnaClassification
FROM
    GLI_TRX_EXTRACT as gte
    JOIN GLG_ACCOUNT as gl on gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
where
    gl.EXTERNAL_GLACCOUNT = '301000001';