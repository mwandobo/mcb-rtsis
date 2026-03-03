SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       gte.TRX_USR                                       AS posNumber,
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
       gte.CURRENCY_SHORT_DES                            AS currency,
       gte.DC_AMOUNT                                     AS orgCurrencyTransactionAmount,
       gte.DC_AMOUNT                                     AS tzsTransactionAmount,
       gte.DC_AMOUNT * 0.18                              AS valueAddedTaxAmount,
       0                                                 AS exciseDutyAmount,
       0                                                 AS electronicLevyAmount

FROM GLI_TRX_EXTRACT gte
         JOIN CUSTOMER as c ON gte.CUST_ID = c.CUST_ID
         JOIN AGENTS_LIST_V3 al
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

WHERE gte.FK_GLG_ACCOUNTACCO IN ('2.3.0.00.0079', '1.4.4.00.0054');
