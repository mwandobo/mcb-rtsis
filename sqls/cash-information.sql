--Cash Information
SELECT varchar_format(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') as reportingDate,
       gte.FK_UNITCODETRXUNIT                            AS branchCode,
       CASE
           WHEN gte.FK_GLG_ACCOUNTACCO = '1.0.1.00.0001' THEN 'Cash in vault'
           WHEN gte.FK_GLG_ACCOUNTACCO = '1.0.1.00.0002' THEN 'Petty cash'
           WHEN gte.FK_GLG_ACCOUNTACCO = '1.0.1.00.0010' OR gte.FK_GLG_ACCOUNTACCO = '1.0.1.00.0015' THEN 'Cash in ATMs'
           WHEN gte.FK_GLG_ACCOUNTACCO = '1.0.1.00.0004' OR gte.FK_GLG_ACCOUNTACCO = '1.0.1.00.0011'
               THEN 'Cash with Tellers'
           ELSE 'Others'
           END                                           as cashCategory,

       CASE
           WHEN gte.FK_GLG_ACCOUNTACCO = '1.0.1.00.0001' THEN 'CleanNotes'
           WHEN gte.FK_GLG_ACCOUNTACCO = '1.0.1.00.0002' OR
                gte.FK_GLG_ACCOUNTACCO = '1.0.1.00.0010' OR
                gte.FK_GLG_ACCOUNTACCO = '1.0.1.00.0004' OR
                gte.FK_GLG_ACCOUNTACCO = '1.0.1.00.0015' OR gte.FK_GLG_ACCOUNTACCO = '1.0.1.00.0011' THEN 'Notes'
           ELSE NULL
           END                                           as cashSubCategory,
       'Business Hours'                                  as cashSubmissionTime,
       gte.CURRENCY_SHORT_DES                            as currency,
       null                                              as cashDenomination,
       null                                              as quantityOfCoinsNotes,
       -- orgAmount: always original DC_AMOUNT
       gte.DC_AMOUNT                                     AS orgAmount,

       -- USD Amount: only if currency is USD, otherwise null
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN gte.DC_AMOUNT
           WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
           WHEN gte.CURRENCY_SHORT_DES <> 'USD'
               THEN DECIMAL(gte.DC_AMOUNT / fx.RATE, 18, 2)
           END                                           AS usdAmount,

       -- TZS Amount: convert only if USD, otherwise use as is
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT * fx.RATE, 18, 2)
           ELSE
               gte.DC_AMOUNT
           END                                           AS tzsAmount,
       VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYHHMM')       as transactionDate,
       varchar_format(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') as maturityDate,
       0                                                 as allowanceProbableLoss,
       0                                                 as botProvision
FROM GLI_TRX_EXTRACT AS gte

         -- Join Currency Using SHORT_DESCR
-- =========================================
         LEFT JOIN CURRENCY curr
                   ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

    -- =============================================
-- Latest Fixing Rate Per Currency
-- =============================================
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
WHERE gte.FK_GLG_ACCOUNTACCO IN
      ('1.0.1.00.0001', '1.0.1.00.0002', '1.0.1.00.0004', '1.0.1.00.0007', '1.0.1.00.0010', '1.0.1.00.0015')
  AND gte.TMSTAMP > :last_timestamp