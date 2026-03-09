-- DB2 Summary Query for Cash Information Pipeline
SELECT COUNT(*) as record_count
FROM CASH_TRANSACTIONS ct
         JOIN CASH_BATCH cb ON cb.BATCH_ID = ct.BATCH_ID
         JOIN CURRENCY curr ON curr.ID_CURRENCY = ct.CURRENCY_ID
         JOIN EMPLOYEE e ON e.EMP_ID = ct.CREATED_BY