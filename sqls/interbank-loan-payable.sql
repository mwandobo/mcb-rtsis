WITH loan_base AS (
    SELECT
        la.CUST_ID,
        la.EOM_DATE,
        la.ACCOUNT_NUMBER,
        la.CUSTOMER_NAME,
        la.LOAN_TYPE_NAME,
        la.LAST_TRANSACTION_DATE,
        la.DRAWDOWN_FST_DT,
        la.ACC_EXP_DT,
        la.CURRENCY,
        la.BOOK_BALANCE,
        la.FINAL_INTEREST,
        la.SELECTED_NORMAL_RA,
        la.SPREAD,

        -- Previous EOM balance = opening balance
        LAG(la.BOOK_BALANCE) OVER (
            PARTITION BY la.ACCOUNT_NUMBER
            ORDER BY la.EOM_DATE
        ) AS opening_balance

    FROM W_EOM_LOAN_ACCOUNT la
)

SELECT
    CURRENT_TIMESTAMP AS reportingDate,
    lb.CUSTOMER_NAME  AS lenderName,
    lb.ACCOUNT_NUMBER AS accountNumber,
    'TANZANIA, UNITED REPUBLIC OF' AS lenderCountry,
    lb.LOAN_TYPE_NAME AS borrowingType,
    lb.LAST_TRANSACTION_DATE AS transactionDate,
    lb.DRAWDOWN_FST_DT AS disbursementDate,
    lb.ACC_EXP_DT AS maturityDate,
    lb.CURRENCY AS currency,

    /* ================= OPENING ================= */
    DECIMAL(lb.opening_balance, 15, 2) AS orgAmountOpening,

    DECIMAL(
        CASE lb.CURRENCY
            WHEN 'USD' THEN lb.opening_balance
            WHEN 'TZS' THEN lb.opening_balance / 2531.00
            ELSE 0
        END,
        15, 2
    ) AS usdAmountOpening,

    DECIMAL(
        CASE lb.CURRENCY
            WHEN 'TZS' THEN lb.opening_balance
            WHEN 'USD' THEN lb.opening_balance * 2531.00
            ELSE 0
        END,
        15, 2
    ) AS tzsAmountOpening,

    /* ================= REPAYMENT ================= */
    DECIMAL(
        GREATEST(COALESCE(lb.opening_balance, 0) - lb.BOOK_BALANCE, 0),
        15, 2
    ) AS orgAmountRepayment,

    DECIMAL(
        CASE lb.CURRENCY
            WHEN 'USD' THEN GREATEST(COALESCE(lb.opening_balance,0) - lb.BOOK_BALANCE,0)
            WHEN 'TZS' THEN GREATEST(COALESCE(lb.opening_balance,0) - lb.BOOK_BALANCE,0) / 2531.00
            ELSE 0
        END,
        15, 2
    ) AS usdAmountRepayment,

    DECIMAL(
        CASE lb.CURRENCY
            WHEN 'TZS' THEN GREATEST(COALESCE(lb.opening_balance,0) - lb.BOOK_BALANCE,0)
            WHEN 'USD' THEN GREATEST(COALESCE(lb.opening_balance,0) - lb.BOOK_BALANCE,0) * 2531.00
            ELSE 0
        END,
        15, 2
    ) AS tzsAmountRepayment,

    /* ================= CLOSING ================= */
    DECIMAL(lb.BOOK_BALANCE, 15, 2) AS orgAmountClosing,

    DECIMAL(
        CASE lb.CURRENCY
            WHEN 'USD' THEN lb.BOOK_BALANCE
            WHEN 'TZS' THEN lb.BOOK_BALANCE / 2531.00
            ELSE 0
        END,
        15, 2
    ) AS usdAmountClosing,

    DECIMAL(
        CASE lb.CURRENCY
            WHEN 'TZS' THEN lb.BOOK_BALANCE
            WHEN 'USD' THEN lb.BOOK_BALANCE * 2531.00
            WHEN 'EUR' THEN lb.BOOK_BALANCE * (1.17 * 2531.00)
            ELSE 0
        END,
        15, 2
    ) AS tzsAmountClosing,

    /* ================= TENURE ================= */
    DAYS(lb.ACC_EXP_DT) - DAYS(lb.DRAWDOWN_FST_DT) AS tenureDays,

    lb.FINAL_INTEREST AS annualInterestRate,

    /* ================= INTEREST TYPE ================= */
    CASE
        WHEN lb.FINAL_INTEREST IS NOT NULL
             AND lb.FINAL_INTEREST <> lb.SELECTED_NORMAL_RA
            THEN 'effective'
        WHEN lb.SPREAD IS NOT NULL
             AND lb.SPREAD <> 0
            THEN 'negotiated'
        ELSE 'nominal'
    END AS interestRateType

FROM loan_base lb
LEFT JOIN GLI_TRX_EXTRACT gte
    ON lb.CUST_ID = gte.CUST_ID
   AND gte.FK_GLG_ACCOUNTACCO = '2.0.4.01.0001'  -- Interbank loan payable GL number

WHERE lb.opening_balance IS NOT NULL;
