SELECT CURRENT_TIMESTAMP             AS reportingDate,
       gte.TRN_DATE                  AS transactionDate,
       pa.ACCOUNT_NUMBER             AS accountNumber,
       gte.CUST_ID                   AS customerIdentificationNumber,

       -- Deposit, Withdraw or Payment
       CASE
           WHEN gl.EXTERNAL_GLACCOUNT IN ('230000087', '230000123', '144000074') THEN 'Withdraw'
           WHEN gl.EXTERNAL_GLACCOUNT = '144000063' THEN 'Deposit'
           WHEN gl.EXTERNAL_GLACCOUNT IN ('504040001', '504040002', '144000046', '230000064') THEN 'Payment'
           END                       AS mobileTransactionType,
       'Mobile Banking Transactions' AS serviceCategory,
       NULL                          AS subServiceCategory,

       --domestic or international

       'domestic'                    AS serviceStatus,

       VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
       TRIM(gte.FK_USRCODE) || '-' ||
       VARCHAR(gte.LINE_NUM) || '-' ||
       VARCHAR(gte.TRN_DATE) || '-' ||
       VARCHAR(gte.TRN_SNUM)         AS transactionRef,

       'MWCOTZTZ'                    AS benBankOrWalletCode,
       NULL                          AS benAccountOrMobileNumber,

       gte.CURRENCY_SHORT_DES        AS currency,

       -- Original amount (as transacted)
       DECIMAL(gte.DC_AMOUNT, 18, 2) AS orgAmount,

       -- Amount converted to TZS
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'TZS'
               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT * 2600, 18, 2)
           ELSE NULL
           END                       AS tzsAmount,

       NULL                          AS valueAddedTaxAmount,
       NULL                          AS exciseDutyAmount,
       NULL                          AS electronicLevyAmount

FROM GLI_TRX_EXTRACT gte
         LEFT JOIN (SELECT *
                    FROM (SELECT wdc.*,
                                 ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUSTOMER_BEGIN_DAT DESC) rn
                          FROM W_DIM_CUSTOMER wdc)
                    WHERE rn = 1) wdc ON wdc.CUST_ID = gte.CUST_ID
           LEFT JOIN (SELECT *
                    FROM (SELECT pa.*,
                                 ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER) rn
                          FROM PROFITS_ACCOUNT pa
                          WHERE PRFT_SYSTEM = 3)
                    WHERE rn = 1) pa ON pa.CUST_ID = gte.CUST_ID
         LEFT JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
WHERE gl.EXTERNAL_GLACCOUNT IN (
                                '230000087',
                                '144000063',
                                '230000064',
                                '504040001',
                                '504040002',
                                '144000046',
                                '144000074',
                                '230000123'
    );
