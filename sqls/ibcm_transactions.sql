-- SELECT
--     CURRENT_DATE AS reportingDate,
--     dt.TRX_DATE AS transactionDate,
--     dt.BANK_INSTRUCTIONS AS lenderName,
--     dt.DB_INSTRUCTIONS AS borrowerName,
--     CASE
--         WHEN dt.TMSTAMP < TIMESTAMP_FORMAT(CHAR(dt.TICKET_DATE) || ' 15:30:00', 'YYYY-MM-DD HH24:MI:SS')
--         THEN 'market'
--         ELSE 'off market'
--     END AS transactionType,
--     CASE
--         WHEN dt.SELL_LEND_AMOUNT > dt.BUY_BORROW_AMOUNT THEN dt.SELL_LEND_AMOUNT
--         ELSE dt.BUY_BORROW_AMOUNT
--     END AS tzsAmount,
--     (DAYS(dt.END_DATE) - DAYS(dt.TRX_DATE)) AS tenure,
--     dt.RATE AS interestRate
-- FROM DEAL_TICKET dt  where dt.CR_BANK_ID <> 0;

SELECT CURRENT_TIMESTAMP       AS reportingDate,
       dt.TRX_DATE             AS transactionDate,
       lender.FIRST_NAME       AS lenderName,
       borrower.FIRST_NAME     AS borrowerName,
       CASE
           WHEN dt.TMSTAMP < TIMESTAMP_FORMAT(CHAR(dt.TICKET_DATE) || ' 15:30:00', 'YYYY-MM-DD HH24:MI:SS')
               THEN 'market'
           ELSE 'off market'
           END                 AS transactionType,
       CASE
           WHEN dt.SELL_LEND_AMOUNT > dt.BUY_BORROW_AMOUNT THEN dt.SELL_LEND_AMOUNT
           ELSE dt.BUY_BORROW_AMOUNT
           END                 AS tzsAmount,
       CASE
           WHEN dt.END_DATE IS NOT NULL THEN (dt.END_DATE - dt.TRX_DATE)
           ELSE NULL
           END                 AS tenure,
       DECIMAL(dt.RATE, 18, 1) AS interestRate
FROM DEAL_TICKET dt
         LEFT JOIN CUSTOMER lender
                   ON lender.CUST_ID = dt.CUST_ID
-- Join to get borrower name
         LEFT JOIN CUSTOMER borrower
                   ON borrower.CUST_ID = dt.CUST_ID
         LEFT JOIN RATE_TABLE fx
                   ON fx.FK_CURRENCYID_CURR = dt.SELL_LEND_CCY;