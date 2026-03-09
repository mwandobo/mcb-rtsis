-- DB2 Summary Query for Balance with MNOs Pipeline
SELECT COUNT(*) as record_count
FROM GLI_TRX_EXTRACT gte
         JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
         JOIN CUSTOMER c ON c.CUST_ID = gte.CUST_ID
         LEFT JOIN CURRENCY cu ON UPPER(TRIM(cu.SHORT_DESCR)) = UPPER(TRIM(gte.CURRENCY_SHORT_DES))
         LEFT JOIN CURRENCY curr ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES
         JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = gte.CUST_ID AND pa.PRFT_SYSTEM = 3
         LEFT JOIN (
             SELECT fr.fk_currencyid_curr, fr.rate
             FROM fixing_rate fr
             WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN
                   (SELECT fk_currencyid_curr, activation_date, MAX(activation_time)
                    FROM fixing_rate
                    WHERE activation_date = (SELECT MAX(b.activation_date)
                                             FROM fixing_rate b
                                             WHERE b.activation_date <= CURRENT_DATE)
                    GROUP BY fk_currencyid_curr, activation_date)
         ) fx ON fx.fk_currencyid_curr = curr.ID_CURRENCY
WHERE gl.EXTERNAL_GLACCOUNT = '100028001'