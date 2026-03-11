select VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') as reportingDate,
       pa.ACCOUNT_NUMBER                                 as accountNumber,
       'BANK OF TANZANIA'                                as accountName,
       'TIPS'                                            as accountType,
       null                                              as subAccountType,
       gte.CURRENCY_SHORT_DES                            as currency,
       gte.DC_AMOUNT                                     AS orgAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'TZS'
               THEN 0
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)

           WHEN gte.CURRENCY_SHORT_DES <> 'USD'
               THEN DECIMAL(gte.DC_AMOUNT / fx.rate, 18, 2)

           ELSE NULL
           END                                           AS usdAmount,

       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT * fx.rate, 18, 2)

           ELSE DECIMAL(gte.DC_AMOUNT, 18, 2)
           END                                           AS tzsAmount,
       VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHHMM')      as transactionDate,
       VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') as maturityDate,
       0                                                 as allowanceProbableLoss,
       0                                                 as botProvision

FROM GLI_TRX_EXTRACT AS gte
         JOIN
     GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
         JOIN CUSTOMER c
              ON c.CUST_ID = gte.CUST_ID
         LEFT JOIN CURRENCY cu
                   ON UPPER(TRIM(cu.SHORT_DESCR)) = UPPER(TRIM(gte.CURRENCY_SHORT_DES))

         LEFT JOIN CURRENCY curr
                   ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

         JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = gte.CUST_ID
    AND pa.PRFT_SYSTEM = 3

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

WHERE gl.EXTERNAL_GLACCOUNT = '100028000' and gte.CUST_ID <> 0
  AND gte.TMSTAMP > :last_timestamp