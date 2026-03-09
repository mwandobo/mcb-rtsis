-- DB2 Summary Query for Agents Pipeline
-- This mirrors the agents pipeline query
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