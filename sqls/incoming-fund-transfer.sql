SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       ORDER_CODE                                        as transactionId,
       VARCHAR_FORMAT(TRX_DATE, 'DDMMYYYYHHMM')          as transactionDate,
       'EFT'                                             as transferChannel,
       NULL                                              AS subCategoryTransferChannel,

       -- Recipient (local customer)
       TRIM(TRIM(cust.FIRST_NAME) || ' ' ||
            CASE
                WHEN TRIM(cust.MIDDLE_NAME) IS NOT NULL
                    AND TRIM(cust.MIDDLE_NAME) <> ''
                    THEN TRIM(cust.MIDDLE_NAME) || ' '
                ELSE ''
                END ||
            TRIM(cust.SURNAME))                          as recipientName,
       im.PRFT_ACCOUNT                                   AS senderAccountNumber,

       CASE
           -- National ID (NIDA) : 20 numeric digits
           WHEN id.ISSUE_AUTHORITY LIKE 'MD%'
               OR id.ISSUE_AUTHORITY LIKE 'DED%'
               OR id.ISSUE_AUTHORITY LIKE 'MKURUGENZI%'
               OR id.ISSUE_AUTHORITY LIKE '%CITY DIRECTOR%'
               OR id.ISSUE_AUTHORITY LIKE '%CITY COUNCIL%'
               THEN 'Employee ID'
           -- Voter Registration Card : starts with T
           WHEN id.ISSUE_AUTHORITY LIKE 'NIDA%'
               THEN 'NationalIdentityCard'
           -- Taxpayer Identification Number (TIN) : 11 digits
           WHEN id.ISSUE_AUTHORITY LIKE 'NEC%' OR id.ISSUE_AUTHORITY LIKE 'NATIONAL ELE%'
               THEN 'VotersRegistrationCard'
           -- Driving Licence (common TZ pattern)
           WHEN id.ISSUE_AUTHORITY LIKE 'BREL%'
               THEN 'Certificate of Incorporation'
           WHEN id.ISSUE_AUTHORITY LIKE 'TRA%'
               THEN 'DrivingLicense'
           ELSE 'Employee ID'
           END
                                                         AS recipientIdentificationType,
       id.ID_NO                                          AS recipientIdentificationNumber,
       'TANZANIA, UNITED REPUBLIC OF'                    AS recipientCountry,
       'Bank Of Tanzania'                                AS senderName,
       'TANZTZTXXXX'                                     AS senderBankOrFspCode,
       im.BENEF_IBAN_ACC                                 AS senderAccountOrWalletNumber,
       'Mobile Banking Transactions'                     AS serviceCategory,
       'Inter-Bank'                                      AS serviceSubCategory,
       curr.SHORT_DESCR                                  as currency,
       ORDER_AMOUNT                                      as orgAmount,
       CASE
           WHEN curr.SHORT_DESCR = 'USD' THEN im.ORDER_AMOUNT
           WHEN curr.SHORT_DESCR = 'TZS' THEN 0
           WHEN curr.SHORT_DESCR <> 'USD' THEN DECIMAL(im.ORDER_AMOUNT * fx.RATE)
           END                                           AS usdAmount,

       CASE
           WHEN curr.SHORT_DESCR = 'TZS' THEN im.ORDER_AMOUNT
           ELSE DECIMAL(im.ORDER_AMOUNT * fx.RATE, 18, 2)
           END
                                                         as tzsAmount,
       REMITTANCE_INFO                                   as senderInstruction,
       'Salaries and wages'                              as purposes

FROM IPS_MESSAGE_HEADER im
         JOIN CUSTOMER cust ON cust.CUST_ID = im.BENEF_CUST_ID
         JOIN OTHER_ID id ON cust.CUST_ID = id.FK_CUSTOMERCUST_ID
         JOIN CURRENCY curr ON curr.ID_CURRENCY = im.ORDER_CURRENCY
         LEFT JOIN (SELECT fr.fk_currencyid_curr,
                           fr.rate
                    FROM fixing_rate fr
                    WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN
                          (SELECT fk_currencyid_curr,
                                  activation_date,
                                  MAX(activation_time)
                           FROM fixing_rate
                           WHERE activation_date = (SELECT MAX(b.activation_date)
                                                    FROM fixing_rate b
                                                    WHERE b.activation_date <= CURRENT_DATE)
                           GROUP BY fk_currencyid_curr, activation_date)) fx
                   ON fx.fk_currencyid_curr = curr.ID_CURRENCY;
