SELECT
    CURRENT_DATE AS reportingDate,
    dt.TRX_DATE AS transactionDate,
    dt.BANK_INSTRUCTIONS AS lenderName,
    dt.DB_INSTRUCTIONS AS borrowerName,
    CASE
        WHEN dt.TMSTAMP < TIMESTAMP_FORMAT(CHAR(dt.TICKET_DATE) || ' 15:30:00', 'YYYY-MM-DD HH24:MI:SS')
        THEN 'market'
        ELSE 'off market'
    END AS transactionType,
    CASE
        WHEN dt.SELL_LEND_AMOUNT > dt.BUY_BORROW_AMOUNT THEN dt.SELL_LEND_AMOUNT
        ELSE dt.BUY_BORROW_AMOUNT
    END AS tzsAmount,
    (DAYS(dt.END_DATE) - DAYS(dt.TRX_DATE)) AS tenure,
    dt.RATE AS interestRate
FROM DEAL_TICKET dt  where dt.CR_BANK_ID <> 0;