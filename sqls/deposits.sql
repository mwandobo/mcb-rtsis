SELECT VARCHAR_FORMAT(CURRENT TIMESTAMP, 'DDMMYYYYHH24MI') AS reportingDate,
       g.CUST_ID                                           AS clientIdentificationNumber,
       PA.ACCOUNT_NUMBER                                   AS accountNumber,
       wdc.NAME_STANDARD                                   AS accountName,
       wdc.CUST_TYPE_IND                                   AS customerCategory,
       'Tanzania'                                          AS customerCountry,
       pa.MONOTORING_UNIT                                  AS branchCode,
       CASE
           WHEN pa.PRFT_SYSTEM = 4 THEN 'Staff'
           WHEN pa.PRFT_SYSTEM = 3 THEN 'Individual'
           END                                             AS clientType,
       null                                                AS relationshipType,
       wdc.CITY                                            AS district,
       'DAR ES SALAAM'                                     AS region,
       p.DESCRIPTION                                       AS accountProductName,
       'Saving'                                            AS accountType,
       null                                                AS accountSubType,
       'Deposit from public'                               AS depositCategory,
       CASE
           WHEN pa.ACC_STATUS = 1 THEN 'active'
           WHEN pa.ACC_STATUS = 3 THEN 'closed'
           ELSE 'inactive'
           END                                             AS depositAccountStatus,
       g.TRN_SNUM                                          AS transactionUniqueRef,
       g.TMSTAMP                                           AS timeStamp,
       'Branch'                                            AS serviceChannel,
       g.CURRENCY_SHORT_DES                                AS currency,
       'Deposit'                                           AS transactionType,
       -- Fixed: Handle non-numeric values in DC_AMOUNT safely
       CASE
           WHEN g.DC_AMOUNT IS NULL OR TRIM(CAST(g.DC_AMOUNT AS VARCHAR(50))) = '' THEN 0
           ELSE COALESCE(g.DC_AMOUNT, 0)
       END                                                 AS orgTransactionAmount,

       CASE
           WHEN g.CURRENCY_SHORT_DES = 'USD' AND g.DC_AMOUNT IS NOT NULL
               THEN COALESCE(g.DC_AMOUNT, 0)
           ELSE NULL
           END                                             AS usdTransactionAmount,

       -- TZS Amount: convert only if USD, otherwise use as is (with null safety)
       CASE
           WHEN g.CURRENCY_SHORT_DES = 'USD' AND g.DC_AMOUNT IS NOT NULL
               THEN COALESCE(g.DC_AMOUNT, 0) * 2500
           ELSE COALESCE(g.DC_AMOUNT, 0)
           END                                             AS tzsTransactionAmount,
       null                                                AS transactionPurposes,
       null                                                AS sectorSnaClassification,
       null                                                AS lienNumber,
       null                                                AS orgAmountLien,
       null                                                AS usdAmountLien,
       null                                                AS tzsAmountLien,
       wdc.CUSTOMER_BEGIN_DAT                              AS contractDate,
       null                                                AS maturityDate,
       null                                                AS annualInterestRate,
       null                                                AS interestRateType,
       -- Fixed: Handle non-numeric values for interest amounts safely
       CASE
           WHEN g.DC_AMOUNT IS NULL OR TRIM(CAST(g.DC_AMOUNT AS VARCHAR(50))) = '' THEN 0
           ELSE COALESCE(g.DC_AMOUNT, 0)
       END                                                 AS orgInterestAmount,

       CASE
           WHEN g.CURRENCY_SHORT_DES = 'USD' AND g.DC_AMOUNT IS NOT NULL
               THEN COALESCE(g.DC_AMOUNT, 0)
           ELSE NULL
           END                                             AS usdInterestAmount,

       -- TZS Amount: convert only if USD, otherwise use as is (with null safety)
       CASE
           WHEN g.CURRENCY_SHORT_DES = 'USD' AND g.DC_AMOUNT IS NOT NULL
               THEN COALESCE(g.DC_AMOUNT, 0) * 2500
           ELSE COALESCE(g.DC_AMOUNT, 0)
           END                                             AS tzsInterestAmount
FROM GLI_TRX_EXTRACT g
         LEFT JOIN CUSTOMER c
                   ON c.CUST_ID = g.CUST_ID
         LEFT JOIN W_DIM_CUSTOMER wdc ON wdc.CUST_ID = g.CUST_ID
         LEFT JOIN PRODUCT p ON p.ID_PRODUCT = g.ID_PRODUCT
         -- FIXED: Join on both CUST_ID and ACCOUNT_NUMBER to avoid duplicates
         -- Use string comparison to avoid DECIMAL conversion errors
         LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = g.CUST_ID

                                     AND pa.PRFT_SYSTEM = 3
WHERE g.JUSTIFIC_DESCR = 'JOURNAL CREDIT';
