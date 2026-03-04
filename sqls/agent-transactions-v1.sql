SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       al.AGENT_ID                                       AS agentId,
       CASE
           WHEN be.EMPL_STATUS = '1' THEN 'Active'
           WHEN be.EMPL_STATUS = '0' THEN 'Inactive'
           ELSE 'Suspended'
           END                                           AS agentStatus,
       VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHHMM')      AS transactionDate,
       VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
       TRIM(gte.FK_USRCODE) || '-' ||
       VARCHAR(gte.LINE_NUM) || '-' ||
       VARCHAR(gte.TRN_DATE) || '-' ||
       VARCHAR(gte.TRN_SNUM)                             AS transactionId,
       CASE gte.FK_GLG_ACCOUNTACCO
           WHEN '2.3.0.00.0079' THEN 'Cash Deposit'
           WHEN '1.4.4.00.0054' THEN 'Cash Withdraw'
           END                                           AS transactionType,
       'Point of Sale'                                   AS serviceChannel,
       NULL                                              AS tillNumber,
       gte.CURRENCY_SHORT_DES                            AS currency,
       gte.DC_AMOUNT                                     AS tzsAmount
FROM GLI_TRX_EXTRACT gte
         JOIN CUSTOMER as c ON gte.CUST_ID = c.CUST_ID
         JOIN AGENTS_LIST_V4 al
              ON
                  CASE
                      WHEN LENGTH(REPLACE(gte.TRX_USR, ' ', '')) > 8
                          THEN RIGHT(REPLACE(gte.TRX_USR, ' ', ''), 8)
                      ELSE REPLACE(gte.TRX_USR, ' ', '')
                      END
                      =
                  CASE
                      WHEN LENGTH(REPLACE(al.TERMINAL_ID, ' ', '')) > 8
                          THEN RIGHT(REPLACE(al.TERMINAL_ID, ' ', ''), 8)
                      ELSE REPLACE(al.TERMINAL_ID, ' ', '')
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
         LEFT JOIN CURRENCY curr
                   ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

WHERE gte.FK_GLG_ACCOUNTACCO IN ('2.3.0.00.0079', '1.4.4.00.0054');
