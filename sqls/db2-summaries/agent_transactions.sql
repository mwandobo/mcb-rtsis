-- DB2 Summary Query for Agent Transactions Pipeline
SELECT COUNT(*) as record_count
FROM GLI_TRX_EXTRACT gte
         JOIN (SELECT CASE
                          WHEN LENGTH(REPLACE(TERMINAL_ID, ' ', '')) > 8
                              THEN RIGHT(REPLACE(TERMINAL_ID, ' ', ''), 8)
                          ELSE REPLACE(TERMINAL_ID, ' ', '')
                          END   AS TERMINAL_ID_NORM,
                      MIN(AGENT_ID) AS AGENT_ID
               FROM AGENTS_LIST_V4
               GROUP BY CASE
                            WHEN LENGTH(REPLACE(TERMINAL_ID, ' ', '')) > 8
                                THEN RIGHT(REPLACE(TERMINAL_ID, ' ', ''), 8)
                            ELSE REPLACE(TERMINAL_ID, ' ', '')
                            END
              ) al
              ON al.TERMINAL_ID_NORM =
                 CASE
                     WHEN LENGTH(REPLACE(gte.TRX_USR, ' ', '')) > 8
                         THEN RIGHT(REPLACE(gte.TRX_USR, ' ', ''), 8)
                     ELSE REPLACE(gte.TRX_USR, ' ', '')
                     END
         JOIN (SELECT STAFF_NO,
                      EMPL_STATUS,
                      ROW_NUMBER() OVER (
                          PARTITION BY STAFF_NO
                          ORDER BY TMSTAMP DESC
                          ) rn
               FROM BANKEMPLOYEE) be
              ON be.STAFF_NO = gte.TRX_USR
                  AND be.rn = 1
WHERE gte.FK_GLG_ACCOUNTACCO IN ('2.3.0.00.0079', '1.4.4.00.0054')