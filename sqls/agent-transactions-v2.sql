-- Agent Transactions v2 - Improved query
-- Changes from v1:
--   1. Added MIN/GROUP BY on AGENTS_LIST_V4 to prevent row multiplication
--      when multiple TERMINAL_IDs map to the same AGENT_ID
--   2. Removed unused LEFT JOIN CURRENCY (no column selected from it)
--   3. Simplified JOIN condition - trim + right(8) applied once in subquery
--   4. ROW_NUMBER() added AFTER all JOINs to guarantee unique transactionId
--   5. transactionId now includes rn suffix for guaranteed uniqueness

WITH agent_txn AS (
    SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
           al.AGENT_ID                                       AS agentId,
           CASE
               WHEN be.EMPL_STATUS = '1' THEN 'Active'
               WHEN be.EMPL_STATUS = '0' THEN 'Inactive'
               ELSE 'Suspended'
               END                                           AS agentStatus,
           gte.TRN_DATE,
           gte.FK_UNITCODETRXUNIT,
           gte.FK_USRCODE,
           gte.LINE_NUM,
           gte.TRN_SNUM,
           VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHHMM')      AS transactionDate,
           CASE gte.FK_GLG_ACCOUNTACCO
               WHEN '2.3.0.00.0079' THEN 'Cash Deposit'
               WHEN '1.4.4.00.0054' THEN 'Cash Withdraw'
               END                                           AS transactionType,
           'Point of Sale'                                   AS serviceChannel,
           CAST(NULL AS VARCHAR(1))                          AS tillNumber,
           gte.CURRENCY_SHORT_DES                            AS currency,
           gte.DC_AMOUNT                                     AS tzsAmount
    FROM GLI_TRX_EXTRACT gte
             JOIN CUSTOMER c ON gte.CUST_ID = c.CUST_ID
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
      AND gte.TMSTAMP > :last_timestamp
),
agent_txn_numbered AS (
    SELECT agent_txn.*,
           ROW_NUMBER() OVER (ORDER BY TRN_DATE, FK_UNITCODETRXUNIT, FK_USRCODE, LINE_NUM, TRN_SNUM) AS rn
    FROM agent_txn
)
SELECT reportingDate,
       agentId,
       agentStatus,
       transactionDate,
       VARCHAR(FK_UNITCODETRXUNIT) || '-' ||
       TRIM(FK_USRCODE) || '-' ||
       VARCHAR(LINE_NUM) || '-' ||
       VARCHAR(TRN_DATE) || '-' ||
       VARCHAR(TRN_SNUM) || '-' ||
       TRIM(CAST(rn AS VARCHAR(10)))                     AS transactionId,
       transactionType,
       serviceChannel,
       tillNumber,
       currency,
       tzsAmount
FROM agent_txn_numbered;
