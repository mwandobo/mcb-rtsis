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
       null                                               as subAccountType,
       gte.CURRENCY_SHORT_DES                             as currency,
       -- orgAmount: always original DC_AMOUNT
       gte.DC_AMOUNT                                      AS orgAmount,

       -- USD Amount: only if currency is USD, otherwise null
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
           WHEN gte.CURRENCY_SHORT_DES IN ('TZ', 'TZS')
               THEN DECIMAL(gte.DC_AMOUNT / 2500.00, 18, 2) -- <<< TZS → USD
           ELSE
               NULL
           END                                            AS usdAmount,

       -- TZS Amount: convert only if USD, otherwise use as is
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT * 2500.00, 18, 2)
           WHEN gte.CURRENCY_SHORT_DES IN ('TZ', 'TZS')
               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
           ELSE
               NULL
           END                                            AS tzsAmount,
       gte.TRN_DATE                                       as transactionDate,
       (DATE(gte.AVAILABILITY_DATE) - DATE(gte.TRN_DATE)) AS pastDueDays,
       0                                                  as allowanceProbableLoss,
       0                                                  as botProvision,
       'Current'                                          as assetsClassificationCategory,
       gte.TRN_DATE                                       as contractDate,
       gte.AVAILABILITY_DATE                              as maturityDate,
       'Highly rated Multilateral Development Banks'      as externalRatingCorrespondentBank,
       NULL                                               as gradesUnratedBanks

FROM GLI_TRX_EXTRACT as gte

         LEFT JOIN
     GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
         LEFT JOIN
     CUSTOMER c ON gte.CUST_ID = c.CUST_ID

         LEFT JOIN
     PROFITS.PROFITS_ACCOUNT pa ON gte.CUST_ID = pa.CUST_ID and PRFT_SYSTEM = 3


where gl.EXTERNAL_GLACCOUNT IN ('100050001','100013000','100050000') AND pa.ACCOUNT_NUMBER <> '';