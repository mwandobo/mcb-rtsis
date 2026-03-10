-- DB2 Summary Query for Balance with MNOs Pipeline
SELECT COUNT(*) as record_count
FROM GLI_TRX_EXTRACT gte
         LEFT JOIN CURRENCY curr ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES
         LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = gte.CUST_ID
WHERE gte.FK_GLG_ACCOUNTACCO IN ('1.4.4.00.0058', '1.4.4.00.0062')