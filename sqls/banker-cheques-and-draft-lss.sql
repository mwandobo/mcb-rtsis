WITH pa_unique AS (
    SELECT DEP_ACC_NUMBER,
           CUST_ID,
           LIMIT_CURRENCY,
           ACCOUNT_NUMBER,
           ROW_NUMBER() OVER (PARTITION BY DEP_ACC_NUMBER ORDER BY ACCOUNT_NUMBER) AS rn
    FROM PROFITS_ACCOUNT
)
SELECT CURRENT_TIMESTAMP AS reportingDate,
       nr.CUST_ID        AS customerIdentificationNumber,
       cu.NAME_STANDARD  AS customerName,
       NULL              AS beneficiaryName,
       NULL              AS checkNumber,
       nr.TRX_DATE       AS transactionDate,
       nr.ISSUE_DATE     AS valueDate,
       NULL              AS maturityDate,
       c.SHORT_DESCR     AS currency,
       nr.CHEQUE_AMOUNT  AS orgAmount,

       /* ---------- USD amount (2 dp) ---------- */
       DECIMAL(
           CASE
               WHEN c.SHORT_DESCR = 'USD'
                   THEN nr.CHEQUE_AMOUNT
               WHEN c.SHORT_DESCR = 'TZS'
                   THEN nr.CHEQUE_AMOUNT / 2500.9
               ELSE 0
           END,
           18, 2
       ) AS usdAmount,

       /* ---------- TZS amount (2 dp) ---------- */
       DECIMAL(
           CASE
               WHEN c.SHORT_DESCR = 'TZS'
                   THEN nr.CHEQUE_AMOUNT
               WHEN c.SHORT_DESCR = 'USD'
                   THEN nr.CHEQUE_AMOUNT * 2500.9
               ELSE 0
           END,
           18, 2
       ) AS tzsAmount

FROM (
    SELECT cbi.*,
           pa.CUST_ID,
           pa.LIMIT_CURRENCY
    FROM CHEQUE_BOOK_ITEM cbi
    LEFT JOIN pa_unique pa
           ON pa.DEP_ACC_NUMBER = cbi.ACCOUNT_NUMBER
          AND pa.rn = 1
) nr
LEFT JOIN PROFITS.W_DIM_CUSTOMER cu
       ON cu.CUST_ID = nr.CUST_ID
LEFT JOIN CURRENCY c
       ON c.ID_CURRENCY = nr.LIMIT_CURRENCY
WHERE nr.CHEQUE_AMOUNT > 0;
