-- DB2 Summary Query for Mobile Banking Pipeline
SELECT COUNT(*) as record_count
FROM TRANSACTIONS tr
         JOIN CUSTOMER c ON c.CUST_ID = tr.CUST_ID
         JOIN TERMINAL t ON t.TERMINAL_ID = tr.TERMINAL_ID
         JOIN MOBILE_BANKING mb ON mb.TRANSACTION_ID = tr.TRANSACTION_ID