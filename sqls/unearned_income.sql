-- ============================================================
-- UNEARNED INCOME AND OTHER DEFERRED CREDITS REPORT
-- Source: PROFITS CBS (GLI_TRX_EXTRACT + GLG_ACCOUNT)
-- GL Accounts:
--   230000051 - Unearned interest on Security (T-bills/T-bonds; narrative
--                confirms postings like "INTEREST EARNED ON TBILLS",
--                "RECORDING OF AFS BOND PURCHASE" - beneficiary is the
--                Government of Tanzania as issuer of the securities)
--   230000060 - Unearned on Loan Fees (per-customer loan processing/
--                application fee amortization; many postings embed the
--                actual customer name in ENTRY_COMMENTS, e.g.
--                "LOAN PROCESSING FEE TO VICTORIA NDANZI")
--
-- NEEDS REVIEW: unearnedIncomeType (D178 lookup) only has 6 values -
-- Advance Rent Payment, Member contribution, Prepaid Insurance Premium,
-- Security Deposit, Deferred Tax Liability, Advance from Treasury - and
-- NONE of them is a genuine semantic match for either of these two GL
-- accounts (both are deferred *income*, not the liability-type items
-- D178 lists). Mapped to the closest available values per business
-- direction ("closest available match" over leaving it unmapped):
--   230000051 -> 'Security Deposit' (weak lexical match only)
--   230000060 -> 'Advance Rent Payment' (closest structural analogy:
--                both are payment received in advance for a service/
--                benefit delivered over time)
-- Business should confirm whether D178 needs a new value added for
-- "unearned interest/fee income" rather than reusing these.
--
-- NEEDS REVIEW: beneficiaryName for 230000060 falls back to the raw GL
-- narrative text (ENTRY_COMMENTS) when no better source is available.
-- Some postings embed a parseable account number (format
-- "UNIT-TYPE-SN-CHECK#..."), but that account-number format does not
-- match PROFITS_ACCOUNT.ACCOUNT_NUMBER in this environment (confirmed
-- live - zero matches), so it cannot be resolved to a real customer
-- record. The raw narrative is used as-is instead of a blank/placeholder
-- value, since for many rows it already contains the customer's name.
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

    /* reportingDate */
    VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI') AS reportingDate,

    /* unearnedIncomeType (D178 lookup) -- see NEEDS REVIEW note above */
    CASE TRIM(gl.EXTERNAL_GLACCOUNT)
        WHEN '230000051' THEN 'Security Deposit'
        WHEN '230000060' THEN 'Advance Rent Payment'
        ELSE 'Advance Rent Payment'
    END AS unearnedIncomeType,

    /* beneficiaryName -- see NEEDS REVIEW note above */
    CASE
        WHEN TRIM(gl.EXTERNAL_GLACCOUNT) = '230000051'
            THEN 'Government of Tanzania'
        ELSE COALESCE(
            NULLIF(TRIM(gte.ENTRY_COMMENTS), ''),
            'Bank Customer'
        )
    END AS beneficiaryName,

    /* transactionDate */
    VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHH24MI') AS transactionDate,

    /* currency */
    COALESCE(NULLIF(TRIM(gte.CURRENCY_SHORT_DES), ''), 'TZS') AS currency,

    /* orgAmount */
    gte.numeric_dc_amount AS orgAmount,

    /* usdAmount (optional) */
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
            THEN 0
        WHEN gte.CURRENCY_SHORT_DES = 'USD'
            THEN DECIMAL(gte.numeric_dc_amount, 18, 2)
        WHEN fx.numeric_rate IS NOT NULL AND fx.numeric_rate <> 0
            THEN DECIMAL(
                DECIMAL(gte.numeric_dc_amount, 18, 2) / DECIMAL(fx.numeric_rate, 18, 6),
                18, 2
            )
        ELSE 0
    END AS usdAmount,

    /* tzsAmount */
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
            THEN DECIMAL(gte.numeric_dc_amount, 18, 2)
        WHEN gte.CURRENCY_SHORT_DES = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(
                DECIMAL(gte.numeric_dc_amount, 18, 2) * DECIMAL(fx.numeric_rate, 18, 6),
                18, 2
            )
        ELSE DECIMAL(gte.numeric_dc_amount, 18, 2)
    END AS tzsAmount,

    /* sectorSnaClassification -- confirmed exact value spelling against
       the authoritative Economic Sector classification list (note:
       "Central Governments" plural, not "Central Government" singular). */
    CASE TRIM(gl.EXTERNAL_GLACCOUNT)
        WHEN '230000051' THEN 'Central Governments'
        ELSE 'Households'
    END AS sectorSnaClassification,

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

LEFT JOIN CURRENCY curr
    ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

LEFT JOIN latest_fx fx
    ON fx.fk_currencyid_curr = curr.ID_CURRENCY

WHERE TRIM(gl.EXTERNAL_GLACCOUNT) IN (
    '230000051',
    '230000060'
)
  AND gte.TRN_DATE IS NOT NULL
  AND TRIM(COALESCE(gte.CURRENCY_SHORT_DES, '')) <> ''
  AND gte.numeric_dc_amount IS NOT NULL;
