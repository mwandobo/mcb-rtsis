-- ============================================================
-- ** DEPRECATED - DO NOT RUN **
-- This GL-posting source was confirmed misleading (see NEEDS REVIEW notes
-- below) and has been replaced by the Bancassurance business unit's own
-- Excel file, which carries real policyNumber/commissionRate values this
-- GL can never provide. The "insuranceCommission" target table is now
-- loaded exclusively via
-- db2-postgres-pipeline/insurance_commission/load_insurance_commission_from_excel.py.
-- Kept here only for historical reference of what was tried and why it
-- didn't work.
-- ============================================================

-- ============================================================
-- INSURANCE COMMISSION REPORT
-- Source: PROFITS CBS (GLI_TRX_EXTRACT + GLG_ACCOUNT)
-- GL Accounts:
--   503010011 - BancAssurance Fee        (GLG_ACCOUNT: "BANCASSUARANCE FEES")
--   503010012 - Credit Life Insurance Fee (GLG_ACCOUNT: "INSURANCE COMMISSION")
--
-- NOTE: this replaces an earlier draft of this file that used
-- gte.CUST_ID directly as policyNumber and hardcoded commissionRate to
-- an empty string - confirmed live that CUST_ID/CUSTOMER_ID_NO/
-- ACCOUNT_NUMBER are all unpopulated (0/blank) for every posting on
-- these two GL accounts, so that approach always produced '0'.
--
-- NEEDS REVIEW: policyNumber is the one field the BoT schema marks
-- `required`, but PROFITS has no insurance-policy master table at all
-- (policies are administered on the insurer's own system, not this core
-- banking system) - there is no way to resolve a real policy number from
-- a GL posting. Falls back to the GL entry's own narrative text
-- (ENTRY_COMMENTS), which for many rows is informative (e.g. "HERITAGE
-- INSURANCE COMMISSION 17.05.2019"), rather than a blank/placeholder
-- value, with a final fallback to 'Unknown' when that narrative is
-- itself blank.
--
-- NEEDS REVIEW: commissionRate has no source anywhere in PROFITS (no
-- commission-rate tracking on GL postings or elsewhere) - defaulted to
-- 0, consistent with how other unresolvable numeric fields were handled
-- elsewhere this session, rather than fabricating a plausible-looking
-- rate.
--
-- DATA QUALITY NOTE: narrative text on this GL is a mix of genuine
-- insurance-commission entries and what look like misposted entries
-- (VAT/excise duty payments, ordinary loan charges) also tagged to this
-- GL in the source ledger. Reported faithfully as posted rather than
-- filtered by narrative-text guessing, consistent with how similar
-- ledger noise was handled in other_capital_accounts.sql/unearned_income.sql.
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

    /* policyNumber -- mandatory (the schema's one `required` field); see
       NEEDS REVIEW note at the top of this file. */
    COALESCE(
        NULLIF(TRIM(gte.ENTRY_COMMENTS), ''),
        'Unknown'
    ) AS policyNumber,

    /* currency */
    COALESCE(NULLIF(TRIM(gte.CURRENCY_SHORT_DES), ''), 'TZS') AS currency,

    /* orgCommissionReceivedAmount */
    gte.numeric_dc_amount AS orgCommissionReceivedAmount,

    /* tzsCommissionReceivedAmount */
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
            THEN DECIMAL(gte.numeric_dc_amount, 18, 2)

        WHEN gte.CURRENCY_SHORT_DES = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(
                DECIMAL(gte.numeric_dc_amount, 18, 2) * DECIMAL(fx.numeric_rate, 18, 6),
                18, 2
            )

        ELSE DECIMAL(gte.numeric_dc_amount, 18, 2)
    END AS tzsCommissionReceivedAmount,

    /* commissionReceivedDate */
    VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHH24MI') AS commissionReceivedDate,

    /* commissionRate -- see NEEDS REVIEW note at the top of this file. */
    0 AS commissionRate,

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
    '503010011',
    '503010012'
)
  AND gte.TRN_DATE IS NOT NULL
  AND TRIM(COALESCE(gte.CURRENCY_SHORT_DES, '')) <> ''
  AND gte.numeric_dc_amount IS NOT NULL;
