select VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                                      AS reportingDate,
       out.REFERENCE_NUMBER                                                                   AS transactionId,
       VARCHAR_FORMAT(out.TRX_DATE, 'DDMMYYYYHHMM')                                           AS transactionDate,
       'EFT'                                                                                  AS transferChannel,
       NULL                                                                                   AS subCategoryTransferChannel,
       out.ISSUER_NAME                                                                        AS senderName,
       out.ORD_CUST_ACCOUNT                                                                   AS senderAccountNumber,
       CASE
           -- National ID (NIDA) : 20 numeric digits
           WHEN LENGTH(out.ISSUER_ID_NUM) = 20
               AND out.ISSUER_ID_NUM NOT LIKE '%[^0-9]%'
               THEN 'NationalIdentityCard'
           -- Voter Registration Card : starts with T
           WHEN out.ISSUER_ID_NUM LIKE 'T%'
               THEN 'VotersRegistrationCard'
           -- Taxpayer Identification Number (TIN) : 11 digits
           WHEN LENGTH(out.ISSUER_ID_NUM) = 11
               AND out.ISSUER_ID_NUM NOT LIKE '%[^0-9]%'
               THEN 'Certificate of Incorporation'
           -- Driving Licence (common TZ pattern)
           WHEN out.ISSUER_ID_NUM LIKE '400%'
               THEN 'DrivingLicence'
           ELSE 'Employee ID'
           END                                                                                AS senderIdentificationType,
       out.ISSUER_ID_NUM                                                                      AS senderIdentificationNumber,
       out.BENEF_NAME                                                                         AS recipientName,
       out.BENEF_PHONE                                                                        AS recipientMobileNumber,
       CASE UPPER(TRIM(out.BENEF_COUNTRY))
           WHEN 'TANZANIA' THEN 'TANZANIA, UNITED REPUBLIC OF'
           WHEN 'TZ' THEN 'TANZANIA, UNITED REPUBLIC OF'
           END                                                                                AS recipientCountry,
       out.PAYEE_SWIFT_ADDRES                                                                 AS recipientBankOrFspCode,
       out.BENEF_ACCOUNT                                                                      AS recipientAccountOrWalletNumber,
       'Mobile banking'                                                                       AS serviceChannel,
       'Mobile banking'                                                                       AS serviceCategory,
       'Inter banking'                                                                        AS serviceSubCategory,
       out.ORDER_PAYABLE_CUR                                                                  AS currency,
       out.ORDER_AMOUNT                                                                       AS orgAmount,
       CASE
           WHEN out.ORDER_PAYABLE_CUR = 'USD' THEN out.ORDER_AMOUNT
           WHEN out.ORDER_PAYABLE_CUR = 'TZS' THEN 0
           WHEN out.ORDER_PAYABLE_CUR <> 'USD' THEN DECIMAL(out.ORDER_AMOUNT / fx.RATE, 18, 2)
           ELSE NULL
           END                                                                                AS usdAmount,
       CASE
           WHEN out.ORDER_PAYABLE_CUR = 'USD'
               THEN DECIMAL(out.ORDER_AMOUNT * fx.rate, 18, 2)
           ELSE DECIMAL(out.ORDER_AMOUNT, 18, 2)
           END                                                                                AS tzsAmount,
       'Salaries and wages'                                                                   AS purposes,
       out.SPECIAL_TERMS                                                                      AS senderInstruction,
       CASE UPPER(TRIM(out.ISSUER_COUNTRY)) WHEN 'TZ' THEN 'TANZANIA, UNITED REPUBLIC OF' END AS transactionPlace
from OUTGOING_ORDERS out
         LEFT JOIN (SELECT fr.fk_currencyid_curr,
                           fr.rate
                    FROM fixing_rate fr
                    WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN (SELECT fk_currencyid_curr,
                                                                                                     activation_date,
                                                                                                     MAX(activation_time)
                                                                                              FROM fixing_rate
                                                                                              WHERE activation_date =
                                                                                                    (SELECT MAX(b.activation_date)
                                                                                                     FROM fixing_rate b
                                                                                                     WHERE b.activation_date <= CURRENT_DATE)
                                                                                              GROUP BY fk_currencyid_curr, activation_date)) fx
                   ON fx.fk_currencyid_curr = out.FK_CURRENCYID_CURR
;