SELECT CURRENT_TIMESTAMP                                  AS reportingDate,
       pa.ACCOUNT_NUMBER                                  as accountNumber,
       c.SURNAME                                          as accountName,
       CASE
           WHEN UPPER(c.FIRST_NAME) = 'ECOBANK' THEN '040'
           WHEN UPPER(c.FIRST_NAME) = 'BOA' THEN '009'
           WHEN UPPER(c.FIRST_NAME) = 'TPB' THEN '048'
           WHEN UPPER(c.FIRST_NAME) = 'TANZANIA POSTAL BANK' THEN '048'
           END                                            AS bankCode,
       'TANZANIA, UNITED REPUBLIC OF'                     as Country,
       'Domestic bank related'                            as relationshipType,
       'Current'                                          as accountType,
       'Normal'                                           as subAccountType,
       gte.CURRENCY_SHORT_DES                             as currency,
       -- orgAmount: always original DC_AMOUNT
       gte.DC_AMOUNT                                      AS orgAmount,

       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'TZS'
               THEN 0
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)

           WHEN gte.CURRENCY_SHORT_DES <> 'USD'
               THEN DECIMAL(gte.DC_AMOUNT / fx.rate, 18, 2)
           END                                            AS usdAmount,

       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT * fx.rate, 18, 2)

           ELSE DECIMAL(gte.DC_AMOUNT, 18, 2)
           END                                            AS tzsAmount,
       gte.TRN_DATE                                       as transactionDate,
       (DATE(gte.AVAILABILITY_DATE) - DATE(gte.TRN_DATE)) AS pastDueDays,
       0                                                  as allowanceProbableLoss,
       0                                                  as botProvision,
       'Current'                                          as assetsClassificationCategory,
       gte.TRN_DATE                                       as contractDate,
       gte.AVAILABILITY_DATE                              as maturityDate,
       'Unrated'                                          as externalRatingCorrespondentBank,
       'Grade B'                                          as gradesUnratedBanks

FROM GLI_TRX_EXTRACT as gte

         LEFT JOIN
     GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
         LEFT JOIN
     CUSTOMER c ON gte.CUST_ID = c.CUST_ID

         LEFT JOIN
     PROFITS.PROFITS_ACCOUNT pa ON gte.CUST_ID = pa.CUST_ID and PRFT_SYSTEM = 3

         -- =========================================
-- Join Currency Using SHORT_DESCR
-- =========================================
         LEFT JOIN CURRENCY curr
                   ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

    -- =========================================
-- Latest Fixing Rate Per Currency
-- =========================================
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
                   ON fx.fk_currencyid_curr = curr.ID_CURRENCY


where gl.EXTERNAL_GLACCOUNT IN ('100050001', '100013000', '100050000')
  AND pa.ACCOUNT_NUMBER <> ''
  AND gte.TMSTAMP > :last_timestamp;