-- ============================================================
-- INTERBANK LOANS PAYABLE v2 - CURRENT PAYABLE CANDIDATE
--
-- Selection rule (validated against the live source on 24-Sep-2026):
--   * Treasury borrowing only: DEAL_OPERATION = 'B'
--   * Active Treasury status: STATUS = '1'
--   * Contract has not matured: MATURITY_DATE >= CURRENT_DATE
--   * A GL posting exists that can be linked through the complete Treasury
--     transaction key; GL 1.0.2.00.0001 is deliberately not used because
--     its linked deals are lending (DEAL_OPERATION = 'L').
--
-- ACC_AMOUNT_1 is exposed as the current closing amount based on the latest
-- Treasury recording. Treasury Finance must confirm this accounting meaning
-- before this preview query replaces the production pipeline source.
-- ============================================================

WITH latest_recording AS (
    SELECT *
    FROM (
        SELECT
            rec.*,
            ROW_NUMBER() OVER (
                PARTITION BY rec.DEAL_NO
                ORDER BY rec.TRX_DATE DESC, rec.RECORDING_SN DESC
            ) AS rn
        FROM TRS_DEAL_RECORDING rec
    ) ranked
    WHERE rn = 1
),
gl_linked_deals AS (
    SELECT DISTINCT rec.DEAL_NO
    FROM TRS_DEAL_RECORDING rec
    JOIN GLI_TRX_EXTRACT gte
      ON gte.TRN_DATE = rec.TRX_DATE
     AND gte.FK_UNITCODETRXUNIT = rec.TRX_UNIT
     AND gte.TRX_USR = rec.TRX_USR
     AND gte.TRX_SN = rec.TRX_SN
     AND gte.TUN_INTERNAL_SN = rec.TUN_INTERNAL_SN
),
country_by_code AS (
    SELECT COUNTRY_CODE, MIN(COUNTRY_NAME) AS COUNTRY_NAME
    FROM COUNTRIES_LOOKUP
    GROUP BY COUNTRY_CODE
),
latest_rate AS (
    SELECT fk_currencyid_curr, rate
    FROM (
        SELECT
            fr.fk_currencyid_curr,
            fr.rate,
            ROW_NUMBER() OVER (
                PARTITION BY fr.fk_currencyid_curr
                ORDER BY fr.activation_date DESC, fr.activation_time DESC
            ) AS rn
        FROM FIXING_RATE fr
        WHERE fr.activation_date <= CURRENT_DATE
    ) ranked
    WHERE rn = 1
),
usd_rate AS (
    SELECT lr.rate
    FROM latest_rate lr
    JOIN CURRENCY c ON c.ID_CURRENCY = lr.fk_currencyid_curr
    WHERE TRIM(c.SHORT_DESCR) = 'USD'
)
SELECT
    VARCHAR_FORMAT(CURRENT TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
    cb.BANK_NAME AS lenderName,
    TRIM(pa.ACCOUNT_NUMBER) AS accountNumber,
    CASE
        WHEN cl.COUNTRY_NAME = 'TANZANIA' THEN 'TANZANIA, UNITED REPUBLIC OF'
        ELSE cl.COUNTRY_NAME
    END AS lenderCountry,
    'BORROWING' AS borrowingType,
    VARCHAR_FORMAT(mm.DEAL_DATE, 'DDMMYYYYHHMM') AS transactionDate,
    VARCHAR_FORMAT(mm.VALUE_DATE, 'DDMMYYYYHHMM') AS disbursementDate,
    VARCHAR_FORMAT(mm.MATURITY_DATE, 'DDMMYYYYHHMM') AS maturityDate,
    TRIM(cur.SHORT_DESCR) AS currency,
    mm.SOURCE_AMOUNT AS orgAmountOpening,
    CASE
        WHEN TRIM(cur.SHORT_DESCR) = 'USD' THEN mm.SOURCE_AMOUNT
        WHEN cur.NATIONAL_FLAG = '1' THEN DECIMAL(mm.SOURCE_AMOUNT / usd.rate, 18, 2)
        ELSE DECIMAL(mm.SOURCE_AMOUNT * src.rate / usd.rate, 18, 2)
    END AS usdAmountOpening,
    CASE
        WHEN cur.NATIONAL_FLAG = '1' THEN mm.SOURCE_AMOUNT
        ELSE DECIMAL(mm.SOURCE_AMOUNT * src.rate, 18, 2)
    END AS tzsAmountOpening,
    mm.SOURCE_AMOUNT - COALESCE(latest.ACC_AMOUNT_1, mm.SOURCE_AMOUNT) AS orgAmountRepayment,
    CASE
        WHEN TRIM(cur.SHORT_DESCR) = 'USD' THEN mm.SOURCE_AMOUNT - COALESCE(latest.ACC_AMOUNT_1, mm.SOURCE_AMOUNT)
        WHEN cur.NATIONAL_FLAG = '1' THEN DECIMAL((mm.SOURCE_AMOUNT - COALESCE(latest.ACC_AMOUNT_1, mm.SOURCE_AMOUNT)) / usd.rate, 18, 2)
        ELSE DECIMAL((mm.SOURCE_AMOUNT - COALESCE(latest.ACC_AMOUNT_1, mm.SOURCE_AMOUNT)) * src.rate / usd.rate, 18, 2)
    END AS usdAmountRepayment,
    CASE
        WHEN cur.NATIONAL_FLAG = '1' THEN mm.SOURCE_AMOUNT - COALESCE(latest.ACC_AMOUNT_1, mm.SOURCE_AMOUNT)
        ELSE DECIMAL((mm.SOURCE_AMOUNT - COALESCE(latest.ACC_AMOUNT_1, mm.SOURCE_AMOUNT)) * src.rate, 18, 2)
    END AS tzsAmountRepayment,
    COALESCE(latest.ACC_AMOUNT_1, mm.SOURCE_AMOUNT) AS orgAmountClosing,
    CASE
        WHEN TRIM(cur.SHORT_DESCR) = 'USD' THEN COALESCE(latest.ACC_AMOUNT_1, mm.SOURCE_AMOUNT)
        WHEN cur.NATIONAL_FLAG = '1' THEN DECIMAL(COALESCE(latest.ACC_AMOUNT_1, mm.SOURCE_AMOUNT) / usd.rate, 18, 2)
        ELSE DECIMAL(COALESCE(latest.ACC_AMOUNT_1, mm.SOURCE_AMOUNT) * src.rate / usd.rate, 18, 2)
    END AS usdAmountClosing,
    CASE
        WHEN cur.NATIONAL_FLAG = '1' THEN COALESCE(latest.ACC_AMOUNT_1, mm.SOURCE_AMOUNT)
        ELSE DECIMAL(COALESCE(latest.ACC_AMOUNT_1, mm.SOURCE_AMOUNT) * src.rate, 18, 2)
    END AS tzsAmountClosing,
    COALESCE(mm.DURATION0, DAYS(mm.MATURITY_DATE) - DAYS(mm.VALUE_DATE)) AS tenureDays,
    DECIMAL(mm.INTEREST_RATE, 15, 2) AS annualInterestRate,
    CASE WHEN mm.PREDETERMINE_FLAG = '1' THEN 'FIXED' ELSE 'FLOATING' END AS interestRateType,
    mm.DEAL_NO AS dealNumber,
    mm.TMSTAMP AS tmstamp
FROM TREASURY_MM_DEAL mm
JOIN latest_recording latest ON latest.DEAL_NO = mm.DEAL_NO
JOIN gl_linked_deals gl ON gl.DEAL_NO = mm.DEAL_NO
JOIN COLLABORATION_BANK cb ON cb.BANK_ID = mm.FK_DEAL_COL_BANK
JOIN CURRENCY cur ON cur.ID_CURRENCY = mm.FK_SOURCE_CURRENCY
LEFT JOIN latest_rate src ON src.fk_currencyid_curr = mm.FK_SOURCE_CURRENCY
LEFT JOIN usd_rate usd ON 1 = 1
LEFT JOIN country_by_code cl ON cl.COUNTRY_CODE = cb.CNTRY_ISO_CODE
LEFT JOIN (
    SELECT ACCOUNT_SER_NUM, MIN(TRIM(ACCOUNT_NUMBER)) AS ACCOUNT_NUMBER
    FROM PROFITS_ACCOUNT
    WHERE TRIM(ACCOUNT_NUMBER) <> ''
    GROUP BY ACCOUNT_SER_NUM
) pa ON pa.ACCOUNT_SER_NUM = mm.FK_CORRESP_DEP_ACC
WHERE mm.DEAL_OPERATION = 'B'
  AND mm.STATUS = '1'
  AND mm.MATURITY_DATE >= CURRENT_DATE
ORDER BY mm.DEAL_NO;
