-- ============================================================
-- BANK AND FINANCIAL INSTITUTIONS PREMISES, FURNITURE AND
-- EQUIPMENT REPORT
-- Source: PROFITS CBS (ASSET_MASTER, the fixed-asset register)
-- GL Accounts:
--   170050001 - Furniture and Fittings (cost)   -> ASSET_MASTER.GL_ACCOUNT
--   171060001 - Acc.Depr-Furnitures (contra)    -> ASSET_MASTER.OPPOSED_GL_ACCOUNT
-- Confirmed live: ASSET_HCATEGORIES.CATEGORY_CODE = 'E3' ("Furniture")
-- has GL_ACCOUNT = '1.7.0.05.0001' (= external code 170050001), and every
-- ASSET_MASTER row in that category carries OPPOSED_GL_ACCOUNT =
-- '1.7.1.06.0001' (= external code 171060001) - an exact match on both
-- GL codes given. 301 assets in this category, all currently
-- STATUS_CODE='OWN' (no disposals in this environment).
--
-- NEEDS REVIEW: orgDepreciation/usdDepreciation/tzsDepreciation
-- (this period's depreciation charge) and orgImpairmentAmount/
-- usdImpairmentAmount/tzsImpairmentAmount are mandatory per the BoT
-- schema, but PROFITS has no source for either: ASSET_MASTER only
-- carries the cumulative DEPRECIATED_VALUE (no period-movement history -
-- ASSET_MASTER_AUDIT, which would normally hold that, has zero rows in
-- this environment), and there is no impairment field anywhere in the
-- asset register. Defaulted to 0 per business direction ("if nothing is
-- there, put a safe default") rather than estimating a figure the bank's
-- own records don't actually contain.
-- ============================================================

WITH params AS (
    SELECT
        CURRENT_DATE AS report_date,
        CURRENT_TIMESTAMP AS report_timestamp
    FROM SYSIBM.SYSDUMMY1
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

    /* assetCategory (D100) -- furniture is always movable */
    'Movable' AS assetCategory,

    /* usagePremisesFurnitureEquipment (D101) */
    'Office facilities' AS usagePremisesFurnitureEquipment,

    /* acquisitionDate -- mandatory; falls back to reportingDate only in
       the (unseen in this data) case AQUISITION_DATE is unavailable. */
    COALESCE(
        VARCHAR_FORMAT(NULLIF(DATE(am.AQUISITION_DATE), DATE('0001-01-01')), 'DDMMYYYYHH24MI'),
        VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI')
    ) AS acquisitionDate,

    /* assetType (D30) -- category E3 is specifically Furniture and Fittings */
    'Furniture and fittings' AS assetType,

    /* currency */
    COALESCE(NULLIF(TRIM(curr.SHORT_DESCR), ''), 'TZS') AS currency,

    /* orgAmount -- original cost */
    COALESCE(am.AMOUNT, 0) AS orgAmount,

    /* usdAmount */
    CASE
        WHEN curr.SHORT_DESCR = 'TZS' THEN 0
        WHEN curr.SHORT_DESCR = 'USD' THEN COALESCE(am.AMOUNT, 0)
        WHEN fx.numeric_rate IS NOT NULL AND fx.numeric_rate <> 0
            THEN DECIMAL(COALESCE(am.AMOUNT, 0) / fx.numeric_rate, 18, 2)
        ELSE 0
    END AS usdAmount,

    /* tzsAmount */
    CASE
        WHEN curr.SHORT_DESCR = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(COALESCE(am.AMOUNT, 0) * fx.numeric_rate, 18, 2)
        ELSE COALESCE(am.AMOUNT, 0)
    END AS tzsAmount,

    /* disposalDate -- optional; populated only when ASSET_STATUS marks
       this asset's status as an actual disposal (sale/trade). TRX_DATE
       is the date of the asset's last status-changing transaction, used
       as the disposal date proxy since ASSET_MASTER has no dedicated
       disposal-date column. */
    CASE
        WHEN ast.INCLUDED_DISPOSALS = '1'
        THEN VARCHAR_FORMAT(NULLIF(DATE(am.TRX_DATE), DATE('0001-01-01')), 'DDMMYYYYHH24MI')
        ELSE NULL
    END AS disposalDate,

    /* orgAssetDisposedValue -- optional; net book value at disposal */
    CASE
        WHEN ast.INCLUDED_DISPOSALS = '1'
        THEN COALESCE(am.DEPRECIAT_VAL_REM, COALESCE(am.AMOUNT, 0) - COALESCE(am.DEPRECIATED_VALUE, 0))
        ELSE NULL
    END AS orgAssetDisposedValue,

    /* usdAssetDisposedValue */
    CASE
        WHEN ast.INCLUDED_DISPOSALS = '1' AND curr.SHORT_DESCR = 'USD'
            THEN COALESCE(am.DEPRECIAT_VAL_REM, COALESCE(am.AMOUNT, 0) - COALESCE(am.DEPRECIATED_VALUE, 0))
        WHEN ast.INCLUDED_DISPOSALS = '1' AND fx.numeric_rate IS NOT NULL AND fx.numeric_rate <> 0
            THEN DECIMAL(COALESCE(am.DEPRECIAT_VAL_REM, COALESCE(am.AMOUNT, 0) - COALESCE(am.DEPRECIATED_VALUE, 0)) / fx.numeric_rate, 18, 2)
        ELSE NULL
    END AS usdAssetDisposedValue,

    /* tzsAssetDisposedValue */
    CASE
        WHEN ast.INCLUDED_DISPOSALS = '1' AND curr.SHORT_DESCR = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(COALESCE(am.DEPRECIAT_VAL_REM, COALESCE(am.AMOUNT, 0) - COALESCE(am.DEPRECIATED_VALUE, 0)) * fx.numeric_rate, 18, 2)
        WHEN ast.INCLUDED_DISPOSALS = '1'
            THEN COALESCE(am.DEPRECIAT_VAL_REM, COALESCE(am.AMOUNT, 0) - COALESCE(am.DEPRECIATED_VALUE, 0))
        ELSE NULL
    END AS tzsAssetDisposedValue,

    /* orgDepreciation / usdDepreciation / tzsDepreciation -- see
       NEEDS REVIEW note at the top of this file. */
    0 AS orgDepreciation,
    0 AS usdDepreciation,
    0 AS tzsDepreciation,

    /* orgAccumDepreciation -- cumulative depreciation to date */
    COALESCE(am.DEPRECIATED_VALUE, 0) AS orgAccumDepreciation,

    /* usdAccumDepreciation */
    CASE
        WHEN curr.SHORT_DESCR = 'TZS' THEN 0
        WHEN curr.SHORT_DESCR = 'USD' THEN COALESCE(am.DEPRECIATED_VALUE, 0)
        WHEN fx.numeric_rate IS NOT NULL AND fx.numeric_rate <> 0
            THEN DECIMAL(COALESCE(am.DEPRECIATED_VALUE, 0) / fx.numeric_rate, 18, 2)
        ELSE 0
    END AS usdAccumDepreciation,

    /* tzsAccumDepreciation */
    CASE
        WHEN curr.SHORT_DESCR = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(COALESCE(am.DEPRECIATED_VALUE, 0) * fx.numeric_rate, 18, 2)
        ELSE COALESCE(am.DEPRECIATED_VALUE, 0)
    END AS tzsAccumDepreciation,

    /* orgImpairmentAmount / usdImpairmentAmount / tzsImpairmentAmount --
       see NEEDS REVIEW note at the top of this file. */
    0 AS orgImpairmentAmount,
    0 AS usdImpairmentAmount,
    0 AS tzsImpairmentAmount,

    /* orgNetBookValue -- DEPRECIAT_VAL_REM is ASSET_MASTER's own
       "remaining undepreciated value" field; falls back to cost minus
       accumulated depreciation if that field is ever unpopulated. */
    COALESCE(am.DEPRECIAT_VAL_REM, COALESCE(am.AMOUNT, 0) - COALESCE(am.DEPRECIATED_VALUE, 0)) AS orgNetBookValue,

    /* usdNetBookValue */
    CASE
        WHEN curr.SHORT_DESCR = 'TZS' THEN 0
        WHEN curr.SHORT_DESCR = 'USD'
            THEN COALESCE(am.DEPRECIAT_VAL_REM, COALESCE(am.AMOUNT, 0) - COALESCE(am.DEPRECIATED_VALUE, 0))
        WHEN fx.numeric_rate IS NOT NULL AND fx.numeric_rate <> 0
            THEN DECIMAL(COALESCE(am.DEPRECIAT_VAL_REM, COALESCE(am.AMOUNT, 0) - COALESCE(am.DEPRECIATED_VALUE, 0)) / fx.numeric_rate, 18, 2)
        ELSE 0
    END AS usdNetBookValue,

    /* tzsNetBookValue */
    CASE
        WHEN curr.SHORT_DESCR = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(COALESCE(am.DEPRECIAT_VAL_REM, COALESCE(am.AMOUNT, 0) - COALESCE(am.DEPRECIATED_VALUE, 0)) * fx.numeric_rate, 18, 2)
        ELSE COALESCE(am.DEPRECIAT_VAL_REM, COALESCE(am.AMOUNT, 0) - COALESCE(am.DEPRECIATED_VALUE, 0))
    END AS tzsNetBookValue,

    /* ============================================================
       SOURCE ROW IDENTITY (not part of the BoT payload)
       ASSET_MASTER's own primary key (ASSET_ID).
       ============================================================ */
    am.ASSET_ID AS sourceAssetId

FROM ASSET_MASTER am

CROSS JOIN params p

LEFT JOIN ASSET_STATUS ast
    ON ast.STATUS_CODE = am.STATUS_CODE

LEFT JOIN CURRENCY curr
    ON curr.ID_CURRENCY = am.CURRENCY_ID

LEFT JOIN latest_fx fx
    ON fx.fk_currencyid_curr = curr.ID_CURRENCY

WHERE am.CATEGORY_CODE = 'E3';
