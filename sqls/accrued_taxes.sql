-- ============================================================
-- ACCRUED TAXES AND OTHER EXPENSES NOT PAID REPORT
-- Source: PROFITS CBS (GLI_TRX_EXTRACT + GLG_ACCOUNT)
-- GL Accounts:
--   220000016 - VAT payable          -> Other taxes payable
--   220000017 - Excise duty payable  -> Other taxes payable
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
)

SELECT

    /* ============================================================
       REPORTING DATE
       ============================================================ */
    VARCHAR_FORMAT(
        p.report_timestamp,
        'DDMMYYYYHH24MI'
    ) AS reportingDate,


    /* ============================================================
       CLAIMANT NAME
       Tax payables (VAT / excise duty) are owed to the Tanzania
       Revenue Authority, not a bank customer or the narrative text
       on the GL entry (ENTRY_COMMENTS/JUSTIFIC_DESCR hold free-text
       transaction notes, not a creditor name).
       ============================================================ */
    'Tanzania Revenue Authority' AS claimantName,


    /* ============================================================
       PAYABLE CATEGORY (D35 lookup)
       ============================================================ */
    CASE
        WHEN gl.EXTERNAL_GLACCOUNT IN (
            '220000016',
            '220000017'
        )
            THEN 'Other taxes payable'

        ELSE 'Other expenses payable'
    END AS payableCategory,


    /* ============================================================
       TRANSACTION DATE
       ============================================================ */
    CASE
        WHEN gte.TRN_DATE IS NOT NULL
        THEN VARCHAR_FORMAT(
            gte.TRN_DATE,
            'DDMMYYYYHH24MI'
        )
        ELSE NULL
    END AS transactionDate,


    /* ============================================================
       CURRENCY
       ============================================================ */
    TRIM(gte.CURRENCY_SHORT_DES) AS currency,


    /* ============================================================
       ORIGINAL AMOUNT
       ============================================================ */
    gte.numeric_dc_amount AS orgAmount,


    /* ============================================================
       USD AMOUNT
       ============================================================ */
    CASE
        WHEN gte.numeric_dc_amount IS NULL
            THEN NULL

        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
            THEN DECIMAL(0, 18, 2)

        WHEN gte.CURRENCY_SHORT_DES = 'USD'
            THEN DECIMAL(
                gte.numeric_dc_amount,
                18,
                2
            )

        WHEN fx.numeric_rate IS NULL
            THEN NULL

        WHEN fx.numeric_rate = 0
            THEN NULL

        ELSE
            DECIMAL(
                DECIMAL(
                    gte.numeric_dc_amount,
                    18,
                    2
                )
                /
                DECIMAL(
                    fx.numeric_rate,
                    18,
                    6
                ),
                18,
                2
            )
    END AS usdAmount,


    /* ============================================================
       TZS AMOUNT
       ============================================================ */
    CASE
        WHEN gte.numeric_dc_amount IS NULL
            THEN NULL

        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
            THEN DECIMAL(
                gte.numeric_dc_amount,
                18,
                2
            )

        WHEN gte.CURRENCY_SHORT_DES = 'USD'
             AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(
                DECIMAL(
                    gte.numeric_dc_amount,
                    18,
                    2
                )
                *
                DECIMAL(
                    fx.numeric_rate,
                    18,
                    6
                ),
                18,
                2
            )

        ELSE
            DECIMAL(
                gte.numeric_dc_amount,
                18,
                2
            )
    END AS tzsAmount,


    /* ============================================================
       MATURITY DATE
       Tax payables are due on demand/remittance; use the entry's
       availability date when present and not already in the past,
       otherwise fall back to the reporting timestamp (maturity can
       never be reported as a past date).
       ============================================================ */
    CASE
        WHEN gte.AVAILABILITY_DATE IS NOT NULL
             AND gte.AVAILABILITY_DATE >= p.report_date
        THEN VARCHAR_FORMAT(
            gte.AVAILABILITY_DATE,
            'DDMMYYYYHH24MI'
        )
        ELSE VARCHAR_FORMAT(
            p.report_timestamp,
            'DDMMYYYYHH24MI'
        )
    END AS maturityDate,


    /* ============================================================
       SECTOR SNA CLASSIFICATION
       Tax obligations are owed to central government regardless
       of any CUST_ID present on the GL entry.
       ============================================================ */
    'Central Government' AS sectorSnaClassification,


    /* ============================================================
       SOURCE ROW IDENTITY (not part of the BoT payload)
       GLI_TRX_EXTRACT's own primary key
       (FK_UNITCODETRXUNIT, FK_USRCODE, LINE_NUM, TRN_DATE, TRN_SNUM).
       Carried through so the pipeline can key each ledger posting
       individually instead of collapsing same-day/same-amount
       postings together (this account sees thousands of distinct
       per-customer duty postings sharing a date and amount).
       ============================================================ */
    gte.FK_UNITCODETRXUNIT AS sourceUnitCode,
    gte.FK_USRCODE         AS sourceUserCode,
    gte.LINE_NUM            AS sourceLineNum,
    gte.TRN_SNUM            AS sourceTrnSnum


FROM clean_gte gte

CROSS JOIN params p

LEFT JOIN GLG_ACCOUNT gl
    ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO

LEFT JOIN CURRENCY curr
    ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

LEFT JOIN latest_fx fx
    ON fx.fk_currencyid_curr = curr.ID_CURRENCY


WHERE gl.EXTERNAL_GLACCOUNT IN (
    '220000016',
    '220000017'
)
  -- Required BoT attributes must come from an authoritative source value.
  AND gte.TRN_DATE IS NOT NULL
  AND TRIM(COALESCE(gte.CURRENCY_SHORT_DES, '')) <> ''
  AND gte.numeric_dc_amount IS NOT NULL;
  -- NOTE: GLI_TRX_EXTRACT carries every posting ever made on this account with
  -- no paid/settled flag to identify what is still outstanding, so this
  -- currently reports full history rather than a true "Not Paid" snapshot.
  -- NEEDS REVIEW: finance/business should confirm how to scope this to
  -- currently-outstanding items (a settlement flag, a lookback window, or
  -- a net-balance calculation) once that's defined.
