-- DB2 Summary Query for Loans Pipeline
SELECT COUNT(DISTINCT la.ACC_SN) as record_count
FROM LOAN_ACCOUNT la
         LEFT JOIN CUSTOMER c ON c.CUST_ID = la.CUST_ID