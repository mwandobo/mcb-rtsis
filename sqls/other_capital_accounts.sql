-- ============================================================
-- OTHER CAPITAL ACCOUNTS REPORT
-- Source: PROFITS CBS (GLI_TRX_EXTRACT + GLG_ACCOUNT)
-- GL Accounts:
--   300000001 - Retained Earnings         -> D38 'Retained earnings'
--   300000002 - Year End Closing          -> D38 'Profit' (P&L sweep into
--                                            equity at year-end; D38 has no
--                                            "Year End Closing" category -
--                                            mapped per business direction)
--   301000001 - Paid up Ordinary Share    -> D38 'Share premium on'
--               Capital                      (parent category) with
--                                             reserveSubCategory 'Ordinary
--                                             share capital'
--   303000004 - Provision General reserve -> D38 'General reserves'
--               (GLG_ACCOUNT's own SHORT_DESCR for this code is
--               "General Reserves", not "General Provision" - used that
--               as the authoritative description)
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
       TRANSACTION DATE
       ============================================================ */
    VARCHAR_FORMAT(
        gte.TRN_DATE,
        'DDMMYYYYHH24MI'
    ) AS transactionDate,


    /* ============================================================
       TRANSACTION TYPE (D161 lookup)
       JUSTIFIC_DESCR carries the GL posting's own debit/credit tag
       ('G/L CREDIT' / 'G/L DEBIT') for these equity accounts. Equity's
       normal balance is credit, so a credit posting is an Addition and
       a debit posting is a Deduction. A small number of rows (13 of 225
       across these 4 GL codes) carry a 'DUMMY' tag instead of a real
       debit/credit marker; those default to Addition since D161 has no
       "Unknown" option and this field is mandatory.
       ============================================================ */
    CASE
        WHEN TRIM(gte.JUSTIFIC_DESCR) = 'G/L DEBIT'
            THEN 'Deduction'
        ELSE 'Addition'
    END AS transactionType,


    /* ============================================================
       RESERVE CATEGORY (D38 lookup)
       ============================================================ */
    CASE TRIM(gl.EXTERNAL_GLACCOUNT)
        WHEN '300000001' THEN 'Retained earnings'
        WHEN '300000002' THEN 'Profit'
        WHEN '301000001' THEN 'Share premium on'
        WHEN '303000004' THEN 'General reserves'
        ELSE 'Others (reserves)'
    END AS reserveCategory,


    /* ============================================================
       RESERVE SUB CATEGORY (D38 lookup, optional)
       Only 301000001 (Paid up Ordinary Share Capital) has a defined
       subcategory under D38's "Share premium on" parent category.
       ============================================================ */
    CASE TRIM(gl.EXTERNAL_GLACCOUNT)
        WHEN '301000001' THEN 'Ordinary share capital'
        ELSE NULL
    END AS reserveSubCategory,


    /* ============================================================
       CURRENCY
       ============================================================ */
    COALESCE(NULLIF(TRIM(gte.CURRENCY_SHORT_DES), ''), 'TZS') AS currency,


    /* ============================================================
       ORIGINAL AMOUNT
       ============================================================ */
    gte.numeric_dc_amount AS orgAmount,


    /* ============================================================
       TZS AMOUNT
       ============================================================ */
    CASE
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
       SOURCE ROW IDENTITY (not part of the BoT payload)
       GLI_TRX_EXTRACT's own primary key
       (FK_UNITCODETRXUNIT, FK_USRCODE, LINE_NUM, TRN_DATE, TRN_SNUM).
       Carried through so the pipeline can key each ledger posting
       individually instead of collapsing postings together.
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


WHERE TRIM(gl.EXTERNAL_GLACCOUNT) IN (
    '300000001',
    '300000002',
    '301000001',
    '303000004'
)
  -- Required BoT attributes must come from an authoritative source value.
  AND gte.TRN_DATE IS NOT NULL
  AND TRIM(COALESCE(gte.CURRENCY_SHORT_DES, '')) <> ''
  AND gte.numeric_dc_amount IS NOT NULL;
