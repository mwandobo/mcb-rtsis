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
       g.TRX_SN                                            AS transactionUniqueRef,
       g.TMSTAMP                                           AS timeStamp,
       'Branch'                                            AS serviceChannel,
       g.CURRENCY_SHORT_DES                                AS currency,
       'Deposit'                                           AS transactionType,
       g.DC_AMOUNT                                         AS orgTransactionAmount,
       CASE
           WHEN g.CURRENCY_SHORT_DES = 'USD' THEN g.DC_AMOUNT
           ELSE NULL
           END                                             AS usdTransactionAmount,

       -- TZS Amount: convert only if USD, otherwise use as is
       CASE
           WHEN g.CURRENCY_SHORT_DES = 'USD'
               THEN g.DC_AMOUNT * 2500
           ELSE
               g.DC_AMOUNT
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
       g.DC_AMOUNT                                         AS orgInterestAmount,
       CASE
           WHEN g.CURRENCY_SHORT_DES = 'USD' THEN g.DC_AMOUNT
           ELSE NULL
           END                                             AS usdInterestAmount,

       -- TZS Amount: convert only if USD, otherwise use as is
       CASE
           WHEN g.CURRENCY_SHORT_DES = 'USD'
               THEN g.DC_AMOUNT * 2500
           ELSE
               g.DC_AMOUNT
           END                                             AS tzsInterestAmount
FROM GLI_TRX_EXTRACT g
         LEFT JOIN CUSTOMER c
                   ON c.CUST_ID = g.CUST_ID
         LEFT JOIN W_DIM_CUSTOMER wdc ON wdc.CUST_ID = g.CUST_ID
         LEFT JOIN PRODUCT p ON p.ID_PRODUCT = g.ID_PRODUCT
         LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = g.CUST_ID AND pa.PRFT_SYSTEM IN (3, 4)
WHERE g.JUSTIFIC_DESCR = 'JOURNAL CREDIT';
