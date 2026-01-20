SELECT CURRENT_TIMESTAMP AS reportingDate,
       gte.CUST_ID                                         AS clientIdentificationNumber,
       PA.ACCOUNT_NUMBER                                   AS accountNumber,
       wdc.NAME_STANDARD                                   AS accountName,
       wdc.CUST_TYPE_IND                                   AS customerCategory,
       'TANZANIA, UNITED REPUBLIC OF'                      AS customerCountry,
       pa.MONOTORING_UNIT                                  AS branchCode,
       CASE
           WHEN pa.PRFT_SYSTEM = 4 THEN 'Staff'
           WHEN pa.PRFT_SYSTEM = 3 THEN 'Individual'
           END                                             AS clientType,
       'Domestic banks unrelated'                          AS relationshipType,
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
       VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
       TRIM(gte.FK_USRCODE) || '-' ||
       VARCHAR(gte.LINE_NUM) || '-' ||
       VARCHAR(gte.TRN_DATE) || '-' ||
       VARCHAR(gte.TRN_SNUM)                               AS transactionUniqueRef,
       gte.TMSTAMP                                         AS timeStamp,
       'Branch'                                            AS serviceChannel,
       gte.CURRENCY_SHORT_DES                              AS currency,
       'Deposit'                                           AS transactionType,
       gte.DC_AMOUNT                                       AS orgTransactionAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
           ELSE NULL
           END                                             AS usdTransactionAmount,

       -- TZS Amount: convert only if USD, otherwise use as is
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN gte.DC_AMOUNT * 2500
           ELSE
               gte.DC_AMOUNT
           END                                             AS tzsTransactionAmount,
       gte.JUSTIFIC_DESCR                                  AS transactionPurposes,
       null                                                AS sectorSnaClassification,
       null                                                AS lienNumber,
       null                                                AS orgAmountLien,
       null                                                AS usdAmountLien,
       null                                                AS tzsAmountLien,
       wdc.CUSTOMER_BEGIN_DAT                              AS contractDate,
       null                                                AS maturityDate,
       null                                                AS annualInterestRate,
       null                                                AS interestRateType,
       0                                                   AS orgInterestAmount,
       0                                                   AS usdInterestAmount,

       -- TZS Amount: convert only if USD, otherwise use as is
       0                                                   AS tzsInterestAmount
FROM GLI_TRX_EXTRACT gte

         LEFT JOIN (SELECT *
                    FROM (SELECT wdc.*,
                                 ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUSTOMER_BEGIN_DAT DESC) rn
                          FROM W_DIM_CUSTOMER wdc)
                    WHERE rn = 1) wdc ON wdc.CUST_ID = gte.CUST_ID
         LEFT JOIN PRODUCT p ON p.ID_PRODUCT = gte.ID_PRODUCT
         LEFT JOIN (SELECT *
                    FROM (SELECT pa.*,
                                 ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER) rn
                          FROM PROFITS_ACCOUNT pa
                          WHERE PRFT_SYSTEM = 3)
                    WHERE rn = 1) pa ON pa.CUST_ID = gte.CUST_ID
WHERE gte.JUSTIFIC_DESCR = 'JOURNAL CREDIT';

