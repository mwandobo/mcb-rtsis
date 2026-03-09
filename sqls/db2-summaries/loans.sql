-- DB2 Summary Query for Loans Pipeline
SELECT COUNT(*) as record_count
FROM LOAN_ACCOUNTS la
         JOIN CUSTOMER c ON c.CUST_ID = la.CUST_ID
         JOIN LOAN_ACCOUNT_DETAILS lad ON lad.LOAN_ACCOUNT_ID = la.LOAN_ACCOUNT_ID
         JOIN LOAN_CLASSIFICATION lc ON lc.LOAN_CLASS_ID = la.LOAN_CLASS_ID
         JOIN LOAN_TYPE lt ON lt.LOAN_TYPE_ID = la.LOAN_TYPE_ID
         JOIN CURRENCY curr ON curr.ID_CURRENCY = la.CURRENCY_ID
         LEFT JOIN COLLATERAL coll ON coll.LOAN_ACCOUNT_ID = la.LOAN_ACCOUNT_ID
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