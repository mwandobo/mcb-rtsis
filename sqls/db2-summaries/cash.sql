-- DB2 Summary Query for Cash Information Pipeline
SELECT COUNT(*) as record_count
FROM GLI_TRX_EXTRACT gte
         LEFT JOIN CURRENCY curr ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES
WHERE gte.FK_GLG_ACCOUNTACCO IN ('1.0.1.00.0001', '1.0.1.00.0002', '1.0.1.00.0004', '1.0.1.00.0007', '1.0.1.00.0010', '1.0.1.00.0015')