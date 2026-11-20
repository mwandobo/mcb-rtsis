-- ============================================================
-- INTERBANK LOANS PAYABLE v1 - GL-LINKAGE DIAGNOSTIC
--
-- Purpose:
--   Test which non-cancelled treasury borrowing deals can be linked to
--   GL 1.0.2.00.0001 through FXFT_GLI_INTERFACE.DEAL_NO.
--
-- Important:
--   This is not a replacement for interbank_loans_payable.sql.  The GL
--   interface is intentionally the driving source in this diagnostic, so
--   deals without a GL-linked transaction are excluded.  GLI_TRX_EXTRACT
--   cannot be used for this join because it has no DEAL_NO column.
--
--   ENTRY_TYPE values in this environment are 1 and 2, not D/C.  Both
--   totals are retained separately; Finance must confirm which represents
--   a repayment before either is used as orgAmountRepayment.
-- ============================================================

WITH gl_by_deal AS (
    SELECT
        fxft.DEAL_NO,
        COUNT(*) AS glTransactionCount,
        MIN(fxft.TRN_DATE) AS firstGlTransactionDate,
        MAX(fxft.TRN_DATE) AS lastGlTransactionDate,
        SUM(CASE WHEN fxft.ENTRY_TYPE = '1' THEN fxft.AMOUNT ELSE 0 END)
            AS entryType1Amount,
        SUM(CASE WHEN fxft.ENTRY_TYPE = '2' THEN fxft.AMOUNT ELSE 0 END)
            AS entryType2Amount
    FROM FXFT_GLI_INTERFACE fxft
    WHERE fxft.FK_GLG_ACCOUNTACCO = '1.0.2.00.0001'
      AND fxft.DEAL_NO IS NOT NULL
    GROUP BY fxft.DEAL_NO
),
country_by_code AS (
    -- COUNTRIES_LOOKUP can contain more than one row for a country code.
    -- Collapse it before joining so each treasury deal is returned once.
    SELECT COUNTRY_CODE, MIN(COUNTRY_NAME) AS COUNTRY_NAME
    FROM COUNTRIES_LOOKUP
    GROUP BY COUNTRY_CODE
)
SELECT
    mm.DEAL_NO AS dealNumber,
    TRIM(pa.ACCOUNT_NUMBER) AS accountNumber,
    cb.BANK_NAME AS lenderName,
    CASE
        WHEN cl.COUNTRY_NAME = 'TANZANIA' THEN 'TANZANIA, UNITED REPUBLIC OF'
        ELSE cl.COUNTRY_NAME
    END AS lenderCountry,
    mm.STATUS AS treasuryStatus,
    TRIM(cur.SHORT_DESCR) AS currency,
    mm.SOURCE_AMOUNT AS orgAmountOpening,
    VARCHAR_FORMAT(mm.DEAL_DATE, 'DDMMYYYYHHMM') AS transactionDate,
    VARCHAR_FORMAT(mm.VALUE_DATE, 'DDMMYYYYHHMM') AS disbursementDate,
    VARCHAR_FORMAT(mm.MATURITY_DATE, 'DDMMYYYYHHMM') AS maturityDate,
    mm.DURATION0 AS tenureDays,
    mm.INTEREST_RATE AS annualInterestRate,
    gl.glTransactionCount,
    VARCHAR_FORMAT(gl.firstGlTransactionDate, 'DDMMYYYYHHMM') AS firstGlTransactionDate,
    VARCHAR_FORMAT(gl.lastGlTransactionDate, 'DDMMYYYYHHMM') AS lastGlTransactionDate,
    gl.entryType1Amount,
    gl.entryType2Amount
FROM gl_by_deal gl
JOIN TREASURY_MM_DEAL mm
  ON mm.DEAL_NO = gl.DEAL_NO
JOIN COLLABORATION_BANK cb
  ON cb.BANK_ID = mm.FK_DEAL_COL_BANK
JOIN CURRENCY cur
  ON cur.ID_CURRENCY = mm.FK_SOURCE_CURRENCY
LEFT JOIN country_by_code cl
  ON cl.COUNTRY_CODE = cb.CNTRY_ISO_CODE
LEFT JOIN (
    SELECT ACCOUNT_SER_NUM, MIN(TRIM(ACCOUNT_NUMBER)) AS ACCOUNT_NUMBER
    FROM PROFITS_ACCOUNT
    WHERE TRIM(ACCOUNT_NUMBER) <> ''
    GROUP BY ACCOUNT_SER_NUM
) pa
  ON pa.ACCOUNT_SER_NUM = mm.FK_CORRESP_DEP_ACC
WHERE mm.DEAL_OPERATION = 'B'
  AND mm.STATUS <> 'C'
ORDER BY mm.DEAL_NO;
