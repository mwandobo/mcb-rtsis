SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP,'DDMMYYYYHHMM') AS reportingDate,
    VARCHAR_FORMAT(CURRENT_TIMESTAMP,'DDMMYYYHHMM') AS floatBalanceDate,

    -- mnoCode based on EXTERNAL_GLACCOUNT
    CASE gl.EXTERNAL_GLACCOUNT
        WHEN '504080001' THEN 'Jumo'
        WHEN '144000051' THEN 'Airtel Money'
        WHEN '144000058' THEN 'Tigo Pesa'
        WHEN '144000061' THEN 'Halopesa'
        WHEN '144000062' THEN 'M-Pesa'
    END AS mnoCode,

    pa.ACCOUNT_NUMBER AS tillNumber,
    gte.CURRENCY_SHORT_DES AS currency,
    0 AS allowanceProbableLoss,
    0 AS botProvision,

    gte.DC_AMOUNT                                      AS orgFloatAmount,

    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'USD'
            THEN DECIMAL(gte.DC_AMOUNT, 18, 2)

        WHEN gte.CURRENCY_SHORT_DES <> 'USD'
            THEN DECIMAL(gte.DC_AMOUNT / fx.rate, 18, 2)

        ELSE NULL
        END                                            AS usdFloatAmount,

    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'USD'
            THEN DECIMAL(gte.DC_AMOUNT * fx.rate, 18, 2)

        ELSE DECIMAL(gte.DC_AMOUNT, 18, 2)
        END                                            AS tzsFloatAmount

FROM GLI_TRX_EXTRACT gte
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

         JOIN GLG_ACCOUNT gl
    ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = gte.CUST_ID
WHERE gl.EXTERNAL_GLACCOUNT IN (
    '504080001','144000051','144000058','144000061','144000062'
);