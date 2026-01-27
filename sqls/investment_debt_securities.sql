SELECT CURRENT_TIMESTAMP                       AS reportingDate,

       pa.ACCOUNT_NUMBER                       AS securityNumber,
       'Treasury bonds'                        AS securityType,
       'Government of Tanzania'                AS securityIssuerName,
       'false'                                 AS ratingStatus,
       'AAA'                                   AS externalIssuerRatting,
       'Grade A'                               AS gradesUnratedBanks,
       'TANZANIA, UNITED REPUBLIC OF'          AS securityIssuerCountry,
       'Other Depository Corporations'         AS sectorSnaClassification,

       COALESCE(gte.CURRENCY_SHORT_DES, 'TZS') AS currency,

       -- =========================
       -- COST VALUES
       -- =========================

       DECIMAL(gte.DC_AMOUNT, 15, 2)           AS orgCostValueAmount,

       DECIMAL(
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT * 2730.50
                   WHEN gte.CURRENCY_SHORT_DES = 'EUR' THEN gte.DC_AMOUNT * 2950.00
                   ELSE gte.DC_AMOUNT
                   END,
               15, 2
       )                                       AS tzsCostValueAmount,

       DECIMAL(
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
                   WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN gte.DC_AMOUNT / 2730.50
                   WHEN gte.CURRENCY_SHORT_DES = 'EUR' THEN gte.DC_AMOUNT * 1.08
                   ELSE NULL
                   END,
               15, 2
       )                                       AS usdCostValueAmount,

       -- =========================
       -- FACE VALUES
       -- =========================

       DECIMAL(gte.DC_AMOUNT, 15, 2)           AS orgFaceValueAmount,

       DECIMAL(
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT * 2730.50
                   WHEN gte.CURRENCY_SHORT_DES = 'EUR' THEN gte.DC_AMOUNT * 2950.00
                   ELSE gte.DC_AMOUNT
                   END,
               15, 2
       )                                       AS tzsgFaceValueAmount,

       DECIMAL(
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
                   WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN gte.DC_AMOUNT / 2730.50
                   WHEN gte.CURRENCY_SHORT_DES = 'EUR' THEN gte.DC_AMOUNT * 1.08
                   ELSE NULL
                   END,
               15, 2
       )                                       AS usdgFaceValueAmount,

       -- =========================
       -- FAIR VALUES
       -- =========================

       DECIMAL(gte.DC_AMOUNT, 15, 2)           AS orgFairValueAmount,

       DECIMAL(
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT * 2730.50
                   WHEN gte.CURRENCY_SHORT_DES = 'EUR' THEN gte.DC_AMOUNT * 2950.00
                   ELSE gte.DC_AMOUNT
                   END,
               15, 2
       )                                       AS tzsgFairValueAmount,

       DECIMAL(
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
                   WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN gte.DC_AMOUNT / 2730.50
                   WHEN gte.CURRENCY_SHORT_DES = 'EUR' THEN gte.DC_AMOUNT * 1.08
                   ELSE NULL
                   END,
               15, 2
       )                                       AS usdgFairValueAmount,

       -- =========================
       -- OTHER FIELDS
       -- =========================

       DECIMAL(0, 9, 6)                        AS interestRate,

       gte.TRN_DATE                            AS purchaseDate,
       gte.AVAILABILITY_DATE                   AS valueDate,
       gte.AVAILABILITY_DATE                   AS maturityDate,

       'Hold to Maturity'                      AS tradingIntent,
       'Unencumbered'                          AS securityEncumbaranceStatus,

       CASE
           WHEN gte.AVAILABILITY_DATE < CURRENT_DATE
               THEN DAYS(CURRENT_DATE) - DAYS(gte.AVAILABILITY_DATE)
           ELSE 0
           END                                 AS pastDueDays,

       DECIMAL(0, 15, 2)                       AS allowanceProbableLoss,
       DECIMAL(0, 15, 2)                       AS botProvision,
       'Current'                               AS assetClassificationCategory

FROM GLI_TRX_EXTRACT gte
         LEFT JOIN GLG_ACCOUNT gl
                   ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
         LEFT JOIN CUSTOMER c
                   ON gte.CUST_ID = c.CUST_ID
         LEFT JOIN PROFITS_ACCOUNT pa
                   ON pa.CUST_ID = gte.CUST_ID

WHERE gte.FK_GLG_ACCOUNTACCO IN (SELECT ACCOUNT_ID
                                 FROM GLG_ACCOUNT
                                 WHERE EXTERNAL_GLACCOUNT LIKE '130%')
  AND gte.DC_AMOUNT > 0
  AND gte.TRN_DATE IS NOT NULL;
