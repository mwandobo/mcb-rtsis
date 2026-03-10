-- ============================================================
-- INTERBANK LOANS PAYABLE REPORT
-- Source: PROFITS CBS (TREASURY_MM_DEAL + FXFT_GLI_INTERFACE)
-- GL Account: 1.0.2.00.0001
-- ============================================================

SELECT
    -- Reporting Date And Time
    VARCHAR_FORMAT(mm.TMSTAMP,'DDMMYYYYHHMM')                                  AS reportingDate,

    -- Lender Name
    cb.BANK_NAME                                AS lenderName,

    -- Account Number (Deal Number / Loan Number)
    mm.DEAL_NO                                  AS accountNumber,

    -- Lender Country
    COALESCE(cl.COUNTRY_NAME, cb.CNTRY_ISO_CODE) AS lenderCountry,

    -- Borrowing Type
    CASE mm.DEAL_OPERATION
        WHEN 'B' THEN 'BORROWING'
        WHEN 'L' THEN 'LENDING'
        ELSE mm.DEAL_OPERATION
    END                                         AS borrowingType,

    -- Transaction Date
    VARCHAR_FORMAT(mm.DEAL_DATE,'DDMMYYYYHHMM')                                AS transactionDate,

    -- Disbursement Date
    VARCHAR_FORMAT(mm.VALUE_DATE,'DDMMYYYYHHMM')               AS disbursementDate,

    -- Maturity Date
    VARCHAR_FORMAT(mm.MATURITY_DATE,'DDMMYYYYHHMM')                            AS maturityDate,

    -- Currency
    cur.SHORT_DESCR                             AS currency,

    -- ===================== OPENING AMOUNTS =====================

    -- Original Amount Opening (in deal currency)
    mm.SOURCE_AMOUNT                            AS orgAmountOpening,

    -- USD Equivalent Amount Opening
    CASE
        WHEN cur.SHORT_DESCR = 'USD' THEN mm.SOURCE_AMOUNT
        WHEN cur.NATIONAL_FLAG = '1' THEN
            CAST(DOUBLE(mm.SOURCE_AMOUNT) / DOUBLE(fr_usd.RATE)
                 AS DECIMAL(15,2))
        ELSE
            CAST(DOUBLE(mm.SOURCE_AMOUNT) * DOUBLE(COALESCE(fr_src.RATE, 1))
                 / DOUBLE(fr_usd.RATE)
                 AS DECIMAL(15,2))
    END                                         AS usdAmountOpening,

    -- TZS Amount Opening
    CASE
        WHEN cur.NATIONAL_FLAG = '1' THEN mm.SOURCE_AMOUNT
        ELSE
            CAST(DOUBLE(mm.SOURCE_AMOUNT) * DOUBLE(COALESCE(fr_src.RATE, 1))
                 AS DECIMAL(15,2))
    END                                         AS tzsAmountOpening,

    -- ===================== REPAYMENT AMOUNTS =====================

    -- Original Amount Repayment (in deal currency)
    COALESCE(repay.org_repayment, 0)            AS orgAmountRepayment,

    -- USD Equivalent Amount Repayment
    CASE
        WHEN cur.SHORT_DESCR = 'USD' THEN COALESCE(repay.org_repayment, 0)
        WHEN cur.NATIONAL_FLAG = '1' THEN
            CAST(DOUBLE(COALESCE(repay.org_repayment, 0))
                 / DOUBLE(fr_usd.RATE)
                 AS DECIMAL(15,2))
        ELSE
            CAST(DOUBLE(COALESCE(repay.org_repayment, 0))
                 * DOUBLE(COALESCE(fr_src.RATE, 1))
                 / DOUBLE(fr_usd.RATE)
                 AS DECIMAL(15,2))
    END                                         AS usdAmountRepayment,

    -- TZS Amount Repayment
    CASE
        WHEN cur.NATIONAL_FLAG = '1' THEN COALESCE(repay.org_repayment, 0)
        ELSE
            CAST(DOUBLE(COALESCE(repay.org_repayment, 0))
                 * DOUBLE(COALESCE(fr_src.RATE, 1))
                 AS DECIMAL(15,2))
    END                                         AS tzsAmountRepayment,

    -- ===================== CLOSING AMOUNTS =====================

    -- Original Amount Closing (Opening - Repayment)
    mm.SOURCE_AMOUNT - COALESCE(repay.org_repayment, 0)
                                                AS orgAmountClosing,

    -- USD Equivalent Amount Closing
    CASE
        WHEN cur.SHORT_DESCR = 'USD' THEN
            mm.SOURCE_AMOUNT - COALESCE(repay.org_repayment, 0)
        WHEN cur.NATIONAL_FLAG = '1' THEN
            CAST(DOUBLE(mm.SOURCE_AMOUNT - COALESCE(repay.org_repayment, 0))
                 / DOUBLE(fr_usd.RATE)
                 AS DECIMAL(15,2))
        ELSE
            CAST(DOUBLE(mm.SOURCE_AMOUNT - COALESCE(repay.org_repayment, 0))
                 * DOUBLE(COALESCE(fr_src.RATE, 1))
                 / DOUBLE(fr_usd.RATE)
                 AS DECIMAL(15,2))
    END                                         AS usdAmountClosing,

    -- TZS Amount Closing
    CASE
        WHEN cur.NATIONAL_FLAG = '1' THEN
            mm.SOURCE_AMOUNT - COALESCE(repay.org_repayment, 0)
        ELSE
            CAST(DOUBLE(mm.SOURCE_AMOUNT - COALESCE(repay.org_repayment, 0))
                 * DOUBLE(COALESCE(fr_src.RATE, 1))
                 AS DECIMAL(15,2))
    END                                         AS tzsAmountClosing,

    -- ===================== TENURE & INTEREST =====================

    -- Tenure Days
    COALESCE(mm.DURATION0,
        DAYS(mm.MATURITY_DATE) - DAYS(mm.VALUE_DATE))
                                                AS tenureDays,

    -- Annual Interest Rate
    DECIMAL(mm.INTEREST_RATE,15,2)                            AS annualInterestRate,

    -- Interest Rate Type (map to Table D102)
    CASE mm.PREDETERMINE_FLAG
        WHEN '1' THEN 'FIXED'
        ELSE 'FLOATING'
    END                                         AS interestRateType

FROM TREASURY_MM_DEAL mm

-- ===================== JOINS =====================

-- Lender bank details
JOIN COLLABORATION_BANK cb
    ON mm.FK_DEAL_COL_BANK = cb.BANK_ID

-- Currency lookup
JOIN CURRENCY cur
    ON mm.FK_SOURCE_CURRENCY = cur.ID_CURRENCY

-- Country name lookup
LEFT JOIN COUNTRIES_LOOKUP cl
    ON cb.CNTRY_ISO_CODE = cl.COUNTRY_CODE

-- ===================== REPAYMENT SUBQUERY =====================
-- Aggregate repayment amounts from FXFT_GLI_INTERFACE
-- on GL account 1.0.2.00.0001 (interbank borrowings)
LEFT JOIN (
    SELECT
        gli.DEAL_NO,
        SUM(CASE WHEN gli.ENTRY_TYPE = 'D' THEN gli.AMOUNT ELSE 0 END)
            AS org_repayment
    FROM FXFT_GLI_INTERFACE gli
    WHERE gli.FK_GLG_ACCOUNTACCO = '1.0.2.00.0001'
      AND gli.DEAL_NO IS NOT NULL
    GROUP BY gli.DEAL_NO
) repay
    ON mm.DEAL_NO = repay.DEAL_NO

-- ===================== FIXING RATES =====================

-- Rate for SOURCE currency -> TZS
-- (e.g. if deal is in EUR, this gives TZS per 1 EUR)
LEFT JOIN FIXING_RATE fr_src
    ON fr_src.FK_CURRENCYID_CURR = mm.FK_SOURCE_CURRENCY
    AND fr_src.ACTIVATION_DATE = (
        SELECT MAX(f1.ACTIVATION_DATE)
        FROM FIXING_RATE f1
        WHERE f1.FK_CURRENCYID_CURR = mm.FK_SOURCE_CURRENCY
          AND f1.ACTIVATION_DATE <= CURRENT DATE
    )

-- Rate for USD -> TZS (always joins to USD regardless of deal currency)
LEFT JOIN CURRENCY cur_usd
    ON cur_usd.SHORT_DESCR = 'USD'
LEFT JOIN FIXING_RATE fr_usd
    ON fr_usd.FK_CURRENCYID_CURR = cur_usd.ID_CURRENCY
    AND fr_usd.ACTIVATION_DATE = (
        SELECT MAX(f2.ACTIVATION_DATE)
        FROM FIXING_RATE f2
        WHERE f2.FK_CURRENCYID_CURR = cur_usd.ID_CURRENCY
          AND f2.ACTIVATION_DATE <= CURRENT DATE
    )

-- ===================== FILTERS =====================

WHERE mm.DEAL_OPERATION = 'B'          -- Borrowings only (loans payable)
  AND mm.STATUS != 'C'                 -- Exclude cancelled deals

ORDER BY mm.DEAL_DATE DESC;
