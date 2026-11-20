WITH src AS (
    SELECT out.*,
           ROW_NUMBER() OVER (ORDER BY out.TMSTAMP)      AS rn,
           COALESCE(out.ORDER_PAYABLE_CUR, 'TZS')         AS clean_currency,
           COALESCE(out.ORDER_AMOUNT, 1000)               AS clean_amount
    FROM OUTGOING_ORDERS out
),
fx_rates AS (
    SELECT fr.fk_currencyid_curr, fr.rate
    FROM fixing_rate fr
    WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN
          (SELECT fk_currencyid_curr,
                  activation_date,
                  MAX(activation_time)
           FROM fixing_rate
           WHERE activation_date =
                 (SELECT MAX(b.activation_date)
                  FROM fixing_rate b
                  WHERE b.activation_date <= CURRENT_DATE)
           GROUP BY fk_currencyid_curr, activation_date)
),
base_data AS (
    SELECT
        VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                        AS reportingDate,

        COALESCE(NULLIF(TRIM(src.REFERENCE_NUMBER), ''), 'REF' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || LPAD(CAST(src.rn AS VARCHAR(10)), 6, '0')) AS transactionId,

        VARCHAR_FORMAT(COALESCE(src.TRX_DATE, CURRENT_TIMESTAMP), 'DDMMYYYYHHMM') AS transactionDate,

        'EFT'                                                                    AS transferChannel,
        NULL                                                                     AS subCategoryTransferChannel,

        COALESCE(NULLIF(TRIM(src.ISSUER_NAME), ''),
            CASE MOD(src.rn, 5)
                WHEN 0 THEN 'Juma Mwakasege'
                WHEN 1 THEN 'Amina Hassan'
                WHEN 2 THEN 'Baraka Mushi'
                WHEN 3 THEN 'Neema Kileo'
                ELSE 'Godfrey Massawe'
            END) AS senderName,

        COALESCE(NULLIF(TRIM(src.ORD_CUST_ACCOUNT), ''), '2201' || LPAD(CAST(src.rn AS VARCHAR(10)), 10, '0')) AS senderAccountNumber,

        CASE
            WHEN LENGTH(src.ISSUER_ID_NUM) = 20 AND src.ISSUER_ID_NUM NOT LIKE '%[^0-9]%' THEN 'NationalIdentityCard'
            WHEN src.ISSUER_ID_NUM LIKE 'T%' THEN 'VotersRegistrationCard'
            WHEN LENGTH(src.ISSUER_ID_NUM) = 11 AND src.ISSUER_ID_NUM NOT LIKE '%[^0-9]%' THEN 'Certificate of Registration'
            WHEN src.ISSUER_ID_NUM LIKE '400%' THEN 'DrivingLicence'
            ELSE 'NationalIdentityCard'
        END AS senderIdentificationType,

        COALESCE(
            NULLIF(TRIM(
                CASE
                    WHEN LENGTH(src.ISSUER_ID_NUM) = 20 AND src.ISSUER_ID_NUM NOT LIKE '%[^0-9]%' THEN src.ISSUER_ID_NUM
                    WHEN src.ISSUER_ID_NUM LIKE 'T%' THEN src.ISSUER_ID_NUM
                    WHEN LENGTH(src.ISSUER_ID_NUM) = 11 AND src.ISSUER_ID_NUM NOT LIKE '%[^0-9]%' THEN src.ISSUER_ID_NUM
                    WHEN src.ISSUER_ID_NUM LIKE '400%' THEN src.ISSUER_ID_NUM
                    ELSE NULL
                END
            ), ''),
            '19900101' || LPAD(CAST(src.rn AS VARCHAR(10)), 12, '0')
        ) AS senderIdentificationNumber,

        COALESCE(NULLIF(TRIM(src.BENEF_NAME), ''),
            CASE MOD(src.rn, 5)
                WHEN 0 THEN 'Halima Ngowi'
                WHEN 1 THEN 'Emmanuel Chuma'
                WHEN 2 THEN 'Zainab Rashid'
                WHEN 3 THEN 'Isaya Mrema'
                ELSE 'Fatuma Said'
            END) AS recipientName,

        src.BENEF_PHONE AS recipientMobileNumber,

        CASE
            WHEN src.BENEF_COUNTRY IS NULL OR TRIM(src.BENEF_COUNTRY) = '' THEN 'TANZANIA, UNITED REPUBLIC OF'
            WHEN UPPER(TRIM(src.BENEF_COUNTRY)) = 'KE' THEN 'KENYA'
            WHEN UPPER(TRIM(src.BENEF_COUNTRY)) IN ('TZ', 'TANZANIA') THEN 'TANZANIA, UNITED REPUBLIC OF'
            ELSE src.BENEF_COUNTRY
        END AS recipientCountry,

        COALESCE(
            NULLIF(TRIM(
                CASE UPPER(TRIM(src.ACC_WITH_BANK_SWIF))
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
                    ELSE src.ACC_WITH_BANK_SWIF
                END
            ), ''),
            'CORUTZTZ'
        ) AS recipientBankOrFspCode,

        COALESCE(NULLIF(TRIM(src.BENEF_ACCOUNT), ''), '3302' || LPAD(CAST(src.rn AS VARCHAR(10)), 10, '0')) AS recipientAccountOrWalletNumber,

        'Mobile banking'   AS serviceChannel,
        'Mobile banking'   AS serviceCategory,
        'Inter banking'    AS serviceSubCategory,

        src.clean_currency AS currency,
        src.clean_amount   AS orgAmount,

        CASE
            WHEN src.clean_currency = 'USD' THEN src.clean_amount
            WHEN src.clean_currency = 'TZS' THEN 0
            ELSE DECIMAL(src.clean_amount / COALESCE(fx.RATE, 2500), 18, 2)
        END AS usdAmount,

        CASE
            WHEN src.clean_currency = 'USD' THEN DECIMAL(src.clean_amount * COALESCE(fx.rate, 2500), 18, 2)
            ELSE DECIMAL(src.clean_amount, 18, 2)
        END AS tzsAmount,

        'Salaries and wages' AS purposes,

        COALESCE(NULLIF(TRIM(src.SPECIAL_TERMS), ''), 'MAINTENANCE EXPENSES FOR FAMILY') AS senderInstruction,

        CASE
            WHEN src.ISSUER_COUNTRY IS NULL OR TRIM(src.ISSUER_COUNTRY) IN ('', '0') THEN 'TANZANIA, UNITED REPUBLIC OF'
            WHEN UPPER(TRIM(src.ISSUER_COUNTRY)) = 'KE' THEN 'KENYA'
            WHEN UPPER(TRIM(src.ISSUER_COUNTRY)) = 'BE' THEN 'BELGIUM'
            WHEN UPPER(TRIM(src.ISSUER_COUNTRY)) IN ('TZ', 'TANZANIA') THEN 'TANZANIA, UNITED REPUBLIC OF'
            ELSE src.ISSUER_COUNTRY
        END AS transactionPlace,

        CASE
            WHEN NULLIF(TRIM(src.REFERENCE_NUMBER), '') IS NULL
              OR src.TRX_DATE IS NULL
              OR NULLIF(TRIM(src.ISSUER_NAME), '') IS NULL
              OR NULLIF(TRIM(src.ORD_CUST_ACCOUNT), '') IS NULL
              OR NULLIF(TRIM(src.BENEF_NAME), '') IS NULL
              OR NULLIF(TRIM(src.BENEF_ACCOUNT), '') IS NULL
              OR NULLIF(TRIM(src.ACC_WITH_BANK_SWIF), '') IS NULL
              OR src.ORDER_PAYABLE_CUR IS NULL
              OR src.ORDER_AMOUNT IS NULL
              OR fx.RATE IS NULL
            THEN 'Y' ELSE 'N'
        END AS isSyntheticData,

        src.TMSTAMP AS tmstamp
    FROM src
    LEFT JOIN fx_rates fx ON fx.fk_currencyid_curr = src.FK_CURRENCYID_CURR
)
SELECT *
FROM base_data
ORDER BY tmstamp;