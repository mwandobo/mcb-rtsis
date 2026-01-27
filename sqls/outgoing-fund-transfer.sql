SELECT CURRENT_TIMESTAMP              AS reportingDate,
       VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
       TRIM(gte.FK_USRCODE) || '-' ||
       VARCHAR(gte.LINE_NUM) || '-' ||
       VARCHAR(gte.TRN_DATE) || '-' ||
       VARCHAR(gte.TRN_SNUM)          AS transactionId,
       gte.TRN_DATE                   AS transactionDate,
       'EFT'                          AS transferChannel,
       NULL                           AS subCategoryTransferChannel,

       pa.ACCOUNT_NUMBER              AS senderAccountNumber,
       CASE
           WHEN LENGTH(TRIM(wdc.ID_NO)) > 18 THEN 'NationalIdentityCard'
           WHEN LENGTH(TRIM(wdc.ID_NO)) BETWEEN 9 AND 11
               AND TRANSLATE(REPLACE(TRIM(wdc.ID_NO), '-', ''), '', '0123456789T') = '' THEN 'DrivingLicense'
           WHEN wdc.ID_NO LIKE 'T%'
               AND TRANSLATE(REPLACE(TRIM(wdc.ID_NO), '-', ''), '', '0123456789T') = ''
               AND LENGTH(TRIM(wdc.ID_NO)) > 10 THEN 'VotersRegistrationCard'
           WHEN wdc.ID_NO LIKE 'AB%' THEN 'Passport'
           ELSE 'Employee ID'
           END                        AS senderIdentificationType,
       wdc.ID_NO                      AS senderIdentificationNumber,
       wdc.NAME_STANDARD              AS recipientName,
       wdc.TELEPHONE                  AS recipientMobileNumber,
       'TANZANIA, UNITED REPUBLIC OF' AS recipientCountry,
       'MWCOTZTZ'                     AS recipientBankOrFspCode,
       NULL                           AS recipientAccountOrWalletNumber,
       'Automated Teller Machines'    AS serviceChannel,
       'Internet banking'             AS serviceCategory,
       'Transfer'                     AS serviceSubCategory,
       gte.CURRENCY_SHORT_DES         AS currency,
       gte.DC_AMOUNT                  AS orgAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
           ELSE DECIMAL(gte.DC_AMOUNT / 2500, 18, 2) -- USD conversion
           END                        AS usdAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN DECIMAL(gte.DC_AMOUNT, 18, 0)
           ELSE DECIMAL(gte.DC_AMOUNT * 2500, 18, 0) -- TZS conversion
           END                        AS tzsAmount,
       gte.JUSTIFIC_DESCR             AS purposes,
       NULL                           AS senderInstruction,
       'TANZANIA, UNITED REPUBLIC OF' AS transactionPlace

FROM GLI_TRX_EXTRACT gte
         LEFT JOIN (SELECT *
                    FROM (SELECT wdc.*, ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUSTOMER_BEGIN_DAT DESC) rn
                          FROM W_DIM_CUSTOMER wdc)
                    WHERE rn = 1) wdc ON wdc.CUST_ID = gte.CUST_ID
         LEFT JOIN (SELECT *
                    FROM (SELECT pa.*, ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER) rn
                          FROM PROFITS_ACCOUNT pa
                          WHERE PRFT_SYSTEM = 3)
                    WHERE rn = 1) pa ON pa.CUST_ID = gte.CUST_ID
         LEFT JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
WHERE gl.EXTERNAL_GLACCOUNT = '100007000'
  AND wdc.ID_NO IS NOT NULL;
