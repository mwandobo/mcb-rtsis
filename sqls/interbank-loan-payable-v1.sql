select VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
--        gte.*,
       TRIM(
               CASE
                   WHEN c.cust_type = '1' THEN
                       TRIM(NVL(c.first_name, '')) || ' ' ||
                       TRIM(NVL(c.middle_name, '')) || ' ' ||
                       TRIM(NVL(c.surname, ''))
                   WHEN c.cust_type = '2' THEN TRIM(c.surname)
                   ELSE ''
                   END
       )                                                 AS lenderName,
       pa.ACCOUNT_NUMBER                                 AS accountNumber,
gte.TMSTAMP
from GLI_TRX_EXTRACT as gte
         JOIN CUSTOMER as c ON gte.CUST_ID = c.CUST_ID
--          LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
--                                    id.fk_customercust_id = c.cust_id)
--
-- --          LEFT JOIN DEPOSIT_ACCOUNT da ON da.FK_CUSTOMERCUST_ID = gte.CUST_ID
         LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = gte.CUST_ID AND pa.PRODUCT_ID  = 12502

WHERE gte.FK_GLG_ACCOUNTACCO IN ('2.0.4.01.0001')
--   AND c.CUST_ID > 0
;