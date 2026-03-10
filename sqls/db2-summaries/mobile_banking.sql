-- DB2 Summary Query for Mobile Banking Pipeline
SELECT COUNT(*) as record_count
FROM GLI_TRX_EXTRACT gte
WHERE gte.FK_GLG_ACCOUNTACCO IN ('2.3.0.00.0087', '1.4.4.00.0063', '2.3.0.00.0064', '5.0.4.04.0001', '5.0.4.04.0002', '1.4.4.00.0046', '1.4.4.00.0074', '2.3.0.00.0123')