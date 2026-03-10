-- DB2 Summary Query for Personal Data Pipeline
SELECT COUNT(*) as record_count
FROM CUSTOMER c
         LEFT JOIN CUST_ADDRESS c_address ON c_address.fk_customercust_id = c.cust_id
             AND c_address.communication_addr = '1'
             AND c_address.entry_status = '1'