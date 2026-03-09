-- DB2 Summary Query for ATM Pipeline
-- This mirrors the ATM pipeline query
SELECT COUNT(*) as record_count
FROM BANKEMPLOYEE be
         JOIN (SELECT STAFF_NO,
                      CASE
                          WHEN STAFF_NO = 'MWL01001' THEN 200
                          ELSE 201
                          END AS branchCode
               FROM BANKEMPLOYEE) b
              ON b.STAFF_NO = be.STAFF_NO
         JOIN UNIT u
              ON u.CODE = b.branchCode
WHERE be.STAFF_NO IS NOT NULL
  AND be.STAFF_NO = TRIM(be.STAFF_NO)
  AND be.EMPL_STATUS = 1
  AND be.STAFF_NO LIKE 'MWL01%'
  AND u.CODE IN (200, 201)