
gte.DC_AMOUNT                                      AS orgAmount,

CASE
    WHEN gte.CURRENCY_SHORT_DES = 'USD'
        THEN DECIMAL(gte.DC_AMOUNT, 18, 2)

    WHEN gte.CURRENCY_SHORT_DES <> 'USD'
        THEN DECIMAL(gte.DC_AMOUNT / fx.rate, 18, 2)

    ELSE NULL
    END                                            AS usdAmount,

CASE
    WHEN gte.CURRENCY_SHORT_DES = 'USD'
        THEN DECIMAL(gte.DC_AMOUNT * fx.rate, 18, 2)

    ELSE DECIMAL(gte.DC_AMOUNT, 18, 2)
    END                                            AS tzsAmount,






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



