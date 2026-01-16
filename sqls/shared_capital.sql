SELECT CURRENT_TIMESTAMP              AS reportingDate,
       'Authorized share capital'     AS capitalCategory,
       'Ordinary share capital'       AS capitalSubCategory,
       gte.TRN_DATE                   AS transactionDate,
       CASE
           WHEN pa.TR_TYPE = 0 THEN 'Addition'
           WHEN pa.TR_TYPE = 1 THEN 'Deduction'
           ELSE 'default'
       END                            AS transactionType,
       wdc.SURNAME,
       NULL                           AS clientType,
       'TANZANIA, UNITED REPUBLIC OF'  AS shareholderCountry,
       NULL                           AS numberOfShares,
       NULL                           AS sharePriceBookValue,
       gte.CURRENCY_SHORT_DES         AS currency,

       -- Original Amount
       CAST(gte.DC_AMOUNT AS DECIMAL(18,2)) AS orgAmount,

       -- USD Amount (ALWAYS populated, 2 dp)
       CAST(
           CASE
               WHEN gte.CURRENCY_SHORT_DES = 'USD'
                   THEN gte.DC_AMOUNT
               WHEN gte.CURRENCY_SHORT_DES = 'TZS'
                   THEN gte.DC_AMOUNT / 2500
               ELSE
                   gte.DC_AMOUNT
           END
           AS DECIMAL(18,2)
       )                              AS usdAmount,

       -- TZS Amount (2 dp)
       CAST(
           CASE
               WHEN gte.CURRENCY_SHORT_DES = 'USD'
                   THEN gte.DC_AMOUNT * 2500
               ELSE
                   gte.DC_AMOUNT
           END
           AS DECIMAL(18,2)
       )                              AS tzsAmount,

       ''                             AS sectorSnaClassification
FROM GLI_TRX_EXTRACT gte
LEFT JOIN CUSTOMER wdc
       ON wdc.CUST_ID = gte.CUST_ID
LEFT JOIN PROFITS_ACCOUNT pa
       ON pa.CUST_ID = gte.CUST_ID
LEFT JOIN GLG_ACCOUNT gl
       ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
WHERE gl.EXTERNAL_GLACCOUNT = '301000001';
