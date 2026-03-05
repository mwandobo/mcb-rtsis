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
               THEN 'Certificate of Registration'
           -- Driving Licence (common TZ pattern)
           WHEN out.ISSUER_ID_NUM LIKE '400%'
               THEN 'DrivingLicence'
           ELSE null
           END                                                                                AS senderIdentificationType,
       out.ISSUER_ID_NUM                                                                      AS senderIdentificationNumber,
       out.BENEF_NAME                                                                         AS recipientName,
       out.BENEF_PHONE                                                                        AS recipientMobileNumber,
       CASE UPPER(TRIM(out.BENEF_COUNTRY)) WHEN 'TZ' THEN 'TANZANIA, UNITED REPUBLIC OF' END  AS recipientCountry,
       CASE UPPER(TRIM(out.ACC_WITH_BANK_SWIF))
           WHEN 'KCBLTZTZXXX' THEN 'KCBLTZTZ'
           WHEN 'ECOCTZTZXXX' THEN 'ECOCTZTZ'
           WHEN 'UCCTTZTZXXX' THEN 'UCCTTZTZF'
           WHEN 'UNAFTZTZXXX' THEN 'UNAFTZTZ'
           WHEN 'ADVBTZTZAXX' THEN 'ADVBTZTZ'
           WHEN 'BOFAUS3DAU2' THEN 'BOFAUS6SXXX'
           WHEN 'NBIMGB21XXX' THEN 'NEIMGB2LXXX'
           WHEN 'CORUTZT10T2' THEN 'CORUTZTZ'
           WHEN 'EQBLTZTZXXX' THEN 'EQBLTZTZ'
           WHEN 'BNKMTZTZXXX' THEN 'MELIDE21XXX'
           WHEN 'TARATZTZXXX' THEN 'TANZTZTXXXX'
           WHEN 'DASUTZTZXXX' THEN 'DASUTZTZ'
           WHEN 'NLCBTZTX0T3' THEN 'NLCBTZTX'
           WHEN 'HABLTZTZXXX' THEN 'HABLTZTZ'
           WHEN 'SFICTZTZXXX' THEN 'SFIPUS31XXX'
           WHEN 'DASUTZTZXXXX' THEN 'DASUTZTZ'
           WHEN 'PBZATZTZXXX' THEN 'PBZATZTZ'
           WHEN 'FNMITZTZXXX' THEN 'FNMITZTZ'
           WHEN 'AKCOTZTZXXX' THEN 'AKCOTZTZ'
           WHEN 'KLMJTZTZXXX' THEN 'KLMJTZTZ'
           WHEN 'FMBZTZTXXXX' THEN 'FMBZTZTX'
           WHEN 'AZANTZTZXXX' THEN 'AZANTZTZ'
           WHEN 'TAPBTZTZXXX' THEN 'TAPBTZTZ'
           WHEN 'NOSCINBBXXX' THEN 'NOSCIE2XXXX'
           WHEN 'NLCBTZTXTAN' THEN 'NLCBTZTX'
           WHEN 'MKCBTZTZXXX' THEN 'MKCBTZTZ'
           WHEN 'HSBCHKHHHKH' THEN 'HSBCHKHHXXX'
           WHEN 'TAINTZTZXXX' THEN 'BKMYTZTZ'
           WHEN 'FIRNTZTXXXX' THEN 'EXTNTZTZF'
           WHEN 'NLCBTZTXZAN' THEN 'NLCBTZTX'
           ELSE out.ACC_WITH_BANK_SWIF
           END                                                                                AS recipientBankOrFspCode,
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
       COALESCE(out.SPECIAL_TERMS, 'MAINTENANCE EXPENSES FOR FAMILY')                         AS senderInstruction,
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