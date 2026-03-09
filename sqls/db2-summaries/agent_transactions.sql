-- DB2 Summary Query for Agent Transactions Pipeline
SELECT COUNT(*) as record_count
FROM AGENTS_LIST_V4 al
         JOIN BANKEMPLOYEE be
              ON
                  CASE
                      WHEN LENGTH(REPLACE(al.TERMINAL_ID, ' ', '')) > 8
                          THEN RIGHT(REPLACE(al.TERMINAL_ID, ' ', ''), 8)
                      ELSE REPLACE(al.TERMINAL_ID, ' ', '')
                      END
                      =
                  CASE
                      WHEN LENGTH(REPLACE(be.STAFF_NO, ' ', '')) > 8
                          THEN RIGHT(REPLACE(be.STAFF_NO, ' ', ''), 8)
                      ELSE REPLACE(be.STAFF_NO, ' ', '')
                      END
         JOIN TRANSACTIONS tr ON tr.TERMINAL_ID = be.STAFF_NO