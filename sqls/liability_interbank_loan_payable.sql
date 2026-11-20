-- ============================================================
-- LIABILITY_INTERBANK_LOAN_PAYABLE REPORT
-- Source: PROFITS CBS (GLI_TRX_EXTRACT + GLG_ACCOUNT)
-- GL Account: 200700001 - Int.Payable-Interbank Local Banks
--             (GLG_ACCOUNT: "INTEREST PAYABLE INTERBANK BOR[ROWINGS]")
--
-- This is a DISTINCT report from the already-submitted "Interbank Loans
-- Payable" (sqls/interbank_loans_payable.sql, GL 204010001 - the
-- principal borrowing). This GL is the accrued-interest-payable
-- sub-ledger instead.
--
-- CONFIRMED DATA GAP (exhaustively checked, see this session's
-- investigation): every one of the 9,305 postings on this GL has
-- CUST_ID=0, ACCOUNT_NUMBER=0, blank ENTRY_COMMENTS, and the identical
-- generic narrative "SAME DAY BORROW LOCAL BANKS" - a daily interest
-- accrual batch process with zero lender/account/deal identity on the
-- ledger rows themselves. Checked every join path available:
--   - FXFT_GLI_INTERFACE.DEAL_NO = 0 for every row on this GL
--   - TRS_DEAL_RECORDING matched via the posting's own transaction
--     fingerprint (TRN_DATE/UNIT/USER/TRX_SN/TUN_INTERNAL_SN): 0 of
--     9,305 postings matched
-- There is no way to attribute an individual posting to a specific
-- lender, account, or loan.
--
-- Built "in a safe way" per explicit instruction: only fields that are
-- genuinely verifiable from the raw ledger (transactionDate, currency,
-- the posting's own amount) are populated from real data. Every field
-- with no real source is set to an UNAMBIGUOUS non-data placeholder
-- ('Unknown' for text, reportingDate itself for dates, 0 for free-form
-- numerics) rather than anything that could be mistaken for a genuine
-- value - consistent with how tbond_transactions.sql was handled.
-- lenderCountry ('Tanzania, United Republic of') and borrowingType
-- ('BORROWING') are the two exceptions: both are true of the GL's own
-- definition/scope (it is specifically interbank-with-local-banks
-- borrowing interest), not a guess about any individual row, so they
-- are populated as constants rather than placeholders.
--
-- orgAmountRepayment/orgAmountClosing (and their usd/tzs equivalents)
-- are set to 0 throughout: a single unlinked daily accrual posting
-- cannot be classified as "opening", "repayment", or "closing" of any
-- specific loan without the loan-level link this GL does not carry.
-- Only orgAmountOpening/usd/tzs reflect the posting's own real amount.
-- ============================================================

WITH params AS (
    SELECT
        CURRENT_DATE AS report_date,
        CURRENT_TIMESTAMP AS report_timestamp
    FROM SYSIBM.SYSDUMMY1
),

clean_gte AS (
    SELECT
        gte.*,

        CASE
            WHEN gte.DC_AMOUNT IS NULL
                THEN NULL

            WHEN TRIM(gte.DC_AMOUNT) = ''
                THEN NULL

            WHEN
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                    TRIM(gte.DC_AMOUNT),
                    '0', ''),
                    '1', ''),
                    '2', ''),
                    '3', ''),
                    '4', ''),
                    '5', ''),
                    '6', ''),
                    '7', ''),
                    '8', ''),
                    '9', ''),
                    ',', ''),
                    '.', ''),
                    '-', ''),
                    '+', '')
                = ''

            THEN DECIMAL(
                REPLACE(TRIM(gte.DC_AMOUNT), ',', ''),
                18,
                2
            )

            ELSE NULL
        END AS numeric_dc_amount

    FROM GLI_TRX_EXTRACT gte
),

latest_fx_raw AS (
    SELECT
        fr.fk_currencyid_curr,
        fr.rate
    FROM fixing_rate fr
    WHERE (
        fr.fk_currencyid_curr,
        fr.activation_date,
        fr.activation_time
    ) IN (
        SELECT
            fk_currencyid_curr,
            activation_date,
            MAX(activation_time)
        FROM fixing_rate
        WHERE activation_date = (
            SELECT MAX(b.activation_date)
            FROM fixing_rate b
            WHERE b.activation_date <= CURRENT_DATE
        )
        GROUP BY
            fk_currencyid_curr,
            activation_date
    )
),

latest_fx AS (
    SELECT
        fk_currencyid_curr,

        CASE
            WHEN rate IS NULL
                THEN NULL

            WHEN TRIM(CHAR(rate)) = ''
                THEN NULL

            WHEN
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                    TRIM(CHAR(rate)),
                    '0', ''),
                    '1', ''),
                    '2', ''),
                    '3', ''),
                    '4', ''),
                    '5', ''),
                    '6', ''),
                    '7', ''),
                    '8', ''),
                    '9', ''),
                    ',', ''),
                    '.', ''),
                    '-', ''),
                    '+', '')
                = ''

            THEN DECIMAL(
                REPLACE(TRIM(CHAR(rate)), ',', ''),
                18,
                6
            )

            ELSE NULL
        END AS numeric_rate

    FROM latest_fx_raw
),

-- fixing_rate only carries entries for foreign currencies (keyed by
-- their own currency id) expressed as TZS-per-unit; TZS itself, being
-- the local base currency, has no row of its own. So the USD rate is
-- fetched once here (not joined per the posting's own currency id,
-- which would find nothing for the ~98% of postings that are TZS) and
-- then applied in whichever direction the posting's currency needs.
latest_usd_rate AS (
    SELECT fx.numeric_rate
    FROM latest_fx fx
    JOIN CURRENCY curr ON curr.ID_CURRENCY = fx.fk_currencyid_curr
    WHERE TRIM(curr.SHORT_DESCR) = 'USD'
      AND fx.numeric_rate IS NOT NULL
    FETCH FIRST 1 ROW ONLY
)

SELECT

    /* reportingDate */
    VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI') AS reportingDate,

    /* lenderName -- no source on this GL; see header note */
    'Unknown' AS lenderName,

    /* accountNumber -- no source on this GL; see header note */
    'Unknown' AS accountNumber,

    /* lenderCountry -- true of the GL's own scope (Interbank Local Banks) */
    'Tanzania, United Republic of' AS lenderCountry,

    /* borrowingType -- true of the GL's own definition (interbank borrowing) */
    'BORROWING' AS borrowingType,

    /* transactionDate -- real, from the posting itself */
    VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHH24MI') AS transactionDate,

    /* disbursementDate -- no source; defaults to transactionDate (not
       reportingDate/today). BOT rejected every row where this fell back to
       today's date with "Unallowed Contract Date After Transaction Date
       Code" - today is always later than a historical posting's own
       transactionDate. Confirmed with BOT support: for pre-existing
       ("previous") contracts, disbursementDate should fall back to
       transactionDate instead. */
    VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHH24MI') AS disbursementDate,

    /* maturityDate -- no source; defaults to reportingDate */
    VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI') AS maturityDate,

    /* currency -- real, from the posting itself */
    COALESCE(NULLIF(TRIM(gte.CURRENCY_SHORT_DES), ''), 'TZS') AS currency,

    /* orgAmountOpening -- the only real amount available: the GL
       posting's own movement amount */
    COALESCE(gte.numeric_dc_amount, 0) AS orgAmountOpening,

    /* usdAmountOpening */
    CASE
        WHEN TRIM(gte.CURRENCY_SHORT_DES) = 'TZS' AND usd.numeric_rate IS NOT NULL AND usd.numeric_rate <> 0
            THEN DECIMAL(COALESCE(gte.numeric_dc_amount, 0) / usd.numeric_rate, 18, 2)
        WHEN TRIM(gte.CURRENCY_SHORT_DES) = 'USD'
            THEN COALESCE(gte.numeric_dc_amount, 0)
        ELSE 0
    END AS usdAmountOpening,

    /* tzsAmountOpening */
    CASE
        WHEN TRIM(gte.CURRENCY_SHORT_DES) = 'USD' AND usd.numeric_rate IS NOT NULL
            THEN DECIMAL(COALESCE(gte.numeric_dc_amount, 0) * usd.numeric_rate, 18, 2)
        ELSE COALESCE(gte.numeric_dc_amount, 0)
    END AS tzsAmountOpening,

    /* orgAmountRepayment / usd / tzs -- see header note: cannot be
       classified without a loan-level link */
    0 AS orgAmountRepayment,
    0 AS usdAmountRepayment,
    0 AS tzsAmountRepayment,

    /* orgAmountClosing / usd / tzs -- see header note */
    0 AS orgAmountClosing,
    0 AS usdAmountClosing,
    0 AS tzsAmountClosing,

    /* tenureDays -- no source; free-form numeric, safe to default to 0 */
    0 AS tenureDays,

    /* annualInterestRate -- no source */
    0 AS annualInterestRate,

    /* interestRateType (D102) -- no source; defaults to the most
       generic valid value */
    'nominal' AS interestRateType,

    /* ============================================================
       SOURCE ROW IDENTITY (not part of the BoT payload)
       GLI_TRX_EXTRACT's own primary key
       (FK_UNITCODETRXUNIT, FK_USRCODE, LINE_NUM, TRN_DATE, TRN_SNUM).
       ============================================================ */
    gte.FK_UNITCODETRXUNIT AS sourceUnitCode,
    gte.FK_USRCODE         AS sourceUserCode,
    gte.LINE_NUM            AS sourceLineNum,
    gte.TRN_SNUM            AS sourceTrnSnum

FROM clean_gte gte

CROSS JOIN params p

LEFT JOIN GLG_ACCOUNT gl
    ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO

LEFT JOIN latest_usd_rate usd
    ON 1 = 1

WHERE TRIM(gl.EXTERNAL_GLACCOUNT) = '200700001'
  AND gte.TRN_DATE IS NOT NULL
  AND TRIM(COALESCE(gte.CURRENCY_SHORT_DES, '')) <> ''
  AND gte.numeric_dc_amount IS NOT NULL;
