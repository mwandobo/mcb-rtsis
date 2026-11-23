SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')      AS reportingDate,
       cfc.CHEQUE_NUMBER                                       AS chequeNumber,
       COALESCE(
           NULLIF(TRIM(
               CASE WHEN ic.FIRST_NAME  IS NOT NULL THEN TRIM(ic.FIRST_NAME)  || ' ' ELSE '' END ||
               CASE WHEN ic.MIDDLE_NAME IS NOT NULL THEN TRIM(ic.MIDDLE_NAME) || ' ' ELSE '' END ||
               CASE WHEN ic.SURNAME     IS NOT NULL THEN TRIM(ic.SURNAME)            ELSE '' END
           ), ''),
           NULLIF(TRIM(cfc.DRAWN_BANK), ''),
           'UNKNOWN'
       )                                                       AS issuerName,
       COALESCE(bic.BIC, TRIM(cfc.DRAWN_BANK))                AS issuerBankerCode,
       TRIM(
           CASE WHEN pc.FIRST_NAME  IS NOT NULL THEN TRIM(pc.FIRST_NAME)  || ' ' ELSE '' END ||
           CASE WHEN pc.MIDDLE_NAME IS NOT NULL THEN TRIM(pc.MIDDLE_NAME) || ' ' ELSE '' END ||
           CASE WHEN pc.SURNAME     IS NOT NULL THEN TRIM(pc.SURNAME)            ELSE '' END
       )                                                       AS payeeName,
       ppa.ACCOUNT_NUMBER                                      AS payeeAccountNumber,
       VARCHAR_FORMAT(cfc.ISSUE_DATE, 'DDMMYYYYHHMM')          AS chequeDate,
       VARCHAR_FORMAT(cfc.TRX_DATE, 'DDMMYYYYHHMM')            AS transactionDate,
       VARCHAR_FORMAT(cfc.BEAR_PAYMENT_DATE, 'DDMMYYYYHHMM')   AS settlementDate,
       0                                                       AS allowanceProbableLoss,
       0                                                       AS botProvision,
       cu.SHORT_DESCR                                          AS currency,
       CAST(COALESCE(da.OPENING_BALANCE, cfc.CHEQUE_AMOUNT) AS DECFLOAT) AS orgAmountOpening,
       0                                                       AS usdAmountOpening,
       CAST(COALESCE(da.OPENING_BALANCE, cfc.CHEQUE_AMOUNT) AS DECFLOAT) AS tzsAmountOpening,
       CAST(cfc.CHEQUE_AMOUNT AS DECFLOAT)                     AS orgAmountPayment,
       0                                                       AS usdAmountPayment,
       CAST(cfc.CHEQUE_AMOUNT AS DECFLOAT)                     AS tzsAmountPayment,
       CAST(COALESCE(da.BOOK_BALANCE, 0) AS DECFLOAT)          AS orgAmountBalance,
       0                                                       AS usdAmountBalance,
       CAST(COALESCE(da.BOOK_BALANCE, 0) AS DECFLOAT)          AS tzsAmountBalance,
       cfc.TMSTAMP                                             AS tmstamp,

       -- Internal-use-only natural key for the underlying cheque collection
       -- record, used solely to prevent duplicate rows on a crash/retry -
       -- not part of the BOT-reported output. Verified unique alone across
       -- the whole table.
       cfc.BILL_SERIAL_NUM                                     AS billSerialNum

FROM CHEQUES_FOR_COLLEC AS cfc

    LEFT JOIN (
        SELECT ACCOUNT_NUMBER,
               MAX(OPENING_BALANCE)    AS OPENING_BALANCE,
               MAX(BOOK_BALANCE)       AS BOOK_BALANCE,
               MAX(FK_CUSTOMERCUST_ID) AS FK_CUSTOMERCUST_ID
        FROM DEPOSIT_ACCOUNT
        GROUP BY ACCOUNT_NUMBER
    ) da ON VARCHAR(da.ACCOUNT_NUMBER) = LTRIM(CHAR(cfc.CHEQUE_NUMBER, 20), '0')

    LEFT JOIN CUSTOMER ic
        ON da.FK_CUSTOMERCUST_ID = ic.CUST_ID

    JOIN CURRENCY cu
        ON cfc.FK_CURRENCYID_CURR = cu.ID_CURRENCY

    JOIN CUSTOMER pc
        ON cfc.FK_CUSTOMERCUST_ID = pc.CUST_ID

    LEFT JOIN BANK_BIC_LOOKUP bic
        ON UPPER(TRIM(cfc.DRAWN_BANK)) = UPPER(TRIM(bic.BANK_NAME))

    LEFT JOIN (
        SELECT CUST_ID, MIN(ACCOUNT_NUMBER) AS ACCOUNT_NUMBER
        FROM PROFITS_ACCOUNT
        GROUP BY CUST_ID
    ) ppa ON ppa.CUST_ID = pc.CUST_ID

ORDER BY cfc.TMSTAMP;