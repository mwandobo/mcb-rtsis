-- DB2 Summary Query for Outgoing Fund Transfer Pipeline
SELECT COUNT(*) as record_count
FROM TRANSACTIONS tr
         JOIN CUSTOMER c ON c.CUST_ID = tr.CUST_ID
         JOIN FUND_TRANSFER ft ON ft.TRANSACTION_ID = tr.TRANSACTION_ID
         JOIN CURRENCY curr ON curr.ID_CURRENCY = tr.CURRENCY_ID
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
WHERE ft.TRANSFER_TYPE = 'OUTGOING'