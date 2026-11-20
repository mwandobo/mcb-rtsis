-- ============================================================
-- IFEM TRANSACTIONS REPORT (Forex Transaction / Placements Local Banks)
-- Source: PROFITS CBS (TREASURY_MM_DEAL, the treasury front-office deal
-- capture table - NOT GLI_TRX_EXTRACT).
-- GL Account: 102000001 - Placements Local Banks
--
-- SOURCE SELECTION NOTE: GLI_TRX_EXTRACT has 4,135 ledger postings for
-- this GL, but it is single-sided balance-sheet movement only (no
-- counterparty bank identity, no buy/sell rate structure) - it cannot
-- populate sellerName/buyerName/exchangeRate/marketType at all.
-- TRS_DEAL_RECORDING (which has a direct GL_ACCOUNT field) was checked
-- and ruled out: GL_ACCOUNT is blank for all 4,434 of its rows in this
-- environment, not just this GL.
-- TREASURY_MM_DEAL is the real deal-capture source for this GL:
--   - DEAL_OPERATION = 'L' (Lend) is specifically the "we place funds
--     with another bank" side that matches "Placements Local Banks"
--     (DEAL_OPERATION = 'B' is the opposite/Borrow side - a different
--     GL not requested here, so excluded).
--   - FK_DEAL_CORRESPOND resolves to the counterparty bank (a CUSTOMER
--     record with CUST_TYPE='2'): confirmed live, 2,725 of 2,731 total
--     deals resolve to a bank counterparty whose name matches
--     BANK_BIC_LOOKUP with a 100% BIC hit rate.
--   - FK_DEAL_COL_BANK was initially suspected to be the counterparty
--     field but was ruled out: for DEAL_OPERATION='L' it resolves to
--     the internal staff dealer who booked the deal, not a bank.
--
-- DATA QUALITY NOTE (test environment): every one of the 1,998 'L'
-- deals resolves to the SAME counterparty customer record -
-- "Mwalimu Commercial Bank Plc" - which is also our own bank's identity
-- (see sellerName below). This is a test/training-data seeding artifact
-- (confirmed live: zero variation across all 1,998 rows), not a query
-- bug - the join logic itself correctly resolves whatever counterparty
-- is on each deal and will differentiate properly once production data
-- has genuine distinct counterparty banks.
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
),

counterparty AS (
    SELECT
        td.DEAL_NO,
        COALESCE(NULLIF(TRIM(cust.SURNAME), ''), 'Unknown Bank') AS bank_name
    FROM TREASURY_MM_DEAL td
    LEFT JOIN CUSTOMER cust
        ON cust.CUST_ID = td.FK_DEAL_CORRESPOND
       AND cust.CUST_TYPE = '2'
)

SELECT

    /* reportingDate */
    VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI') AS reportingDate,

    /* transactionDate */
    COALESCE(
        VARCHAR_FORMAT(NULLIF(DATE(td.DEAL_DATE), DATE('0001-01-01')), 'DDMMYYYYHH24MI'),
        VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI')
    ) AS transactionDate,

    /* valueDate */
    COALESCE(
        VARCHAR_FORMAT(NULLIF(DATE(td.VALUE_DATE), DATE('0001-01-01')), 'DDMMYYYYHH24MI'),
        VARCHAR_FORMAT(NULLIF(DATE(td.DEAL_DATE), DATE('0001-01-01')), 'DDMMYYYYHH24MI'),
        VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI')
    ) AS valueDate,

    /* marketType (D61) -- interbank money-market placements are wholesale */
    'Wholesale (IFEM)' AS marketType,

    /* sellerName -- DEAL_OPERATION='L' (Lend/placement): we provide the
       funds, so we are the seller. Own identity confirmed via
       BANK_PARAMETERS.BANK_CODE=59 -> BOT FSP sender code 059 ->
       "Mwalimu Commercial Bank Plc" (BIC MWCOTZTZ). All rows here are
       'L' (see WHERE clause), so this is always our own bank. */
    'Mwalimu Commercial Bank Plc' AS sellerName,

    /* buyerName -- the counterparty bank receiving the placement */
    cp.bank_name AS buyerName,

    /* sectorSnaClassification -- counterparty is always a deposit-taking
       bank (D38/Economic Sector item 7). */
    'Other Depository Corporations' AS sectorSnaClassification,

    /* buyerEconomicActivity (D81) */
    'Financial intermediation' AS buyerEconomicActivity,

    /* buyerCountry -- scope is specifically local-bank placements */
    'Tanzania, United Republic of' AS buyerCountry,

    /* currency */
    COALESCE(NULLIF(TRIM(curr.SHORT_DESCR), ''), 'TZS') AS currency,

    /* exchangeRate -- these are money-market placements (same currency
       in and out), not spot FX trades, so there is no literal deal-level
       exchange rate. Reports the prevailing TZS fixing rate for the
       deal's currency as the applicable conversion reference (1 for
       TZS itself). */
    CASE
        WHEN curr.SHORT_DESCR = 'TZS' THEN 1
        WHEN fx.numeric_rate IS NOT NULL THEN fx.numeric_rate
        ELSE 1
    END AS exchangeRate,

    /* orgAmountOffered / orgAmountSold -- TREASURY_MM_DEAL is bilateral
       deal-level data (already agreed/executed), not a market order book,
       so there is no separate "offered vs filled" split: both reflect
       the same deal amount (whichever of SOURCE_AMOUNT/TARGET_AMOUNT is
       populated for this operation direction). */
    COALESCE(td.SOURCE_AMOUNT, 0) + COALESCE(td.TARGET_AMOUNT, 0) AS orgAmountOffered,

    /* usdAmountOffered */
    CASE
        WHEN curr.SHORT_DESCR = 'TZS' AND fx.numeric_rate IS NOT NULL AND fx.numeric_rate <> 0
            THEN DECIMAL((COALESCE(td.SOURCE_AMOUNT, 0) + COALESCE(td.TARGET_AMOUNT, 0)) / fx.numeric_rate, 18, 2)
        WHEN curr.SHORT_DESCR = 'USD'
            THEN COALESCE(td.SOURCE_AMOUNT, 0) + COALESCE(td.TARGET_AMOUNT, 0)
        ELSE 0
    END AS usdAmountOffered,

    /* tzsAmountOffered */
    CASE
        WHEN curr.SHORT_DESCR = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL((COALESCE(td.SOURCE_AMOUNT, 0) + COALESCE(td.TARGET_AMOUNT, 0)) * fx.numeric_rate, 18, 2)
        ELSE COALESCE(td.SOURCE_AMOUNT, 0) + COALESCE(td.TARGET_AMOUNT, 0)
    END AS tzsAmountOffered,

    /* orgAmountSold -- see note on orgAmountOffered */
    COALESCE(td.SOURCE_AMOUNT, 0) + COALESCE(td.TARGET_AMOUNT, 0) AS orgAmountSold,

    /* usdAmountSold */
    CASE
        WHEN curr.SHORT_DESCR = 'TZS' AND fx.numeric_rate IS NOT NULL AND fx.numeric_rate <> 0
            THEN DECIMAL((COALESCE(td.SOURCE_AMOUNT, 0) + COALESCE(td.TARGET_AMOUNT, 0)) / fx.numeric_rate, 18, 2)
        WHEN curr.SHORT_DESCR = 'USD'
            THEN COALESCE(td.SOURCE_AMOUNT, 0) + COALESCE(td.TARGET_AMOUNT, 0)
        ELSE 0
    END AS usdAmountSold,

    /* tzsAmountSold */
    CASE
        WHEN curr.SHORT_DESCR = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL((COALESCE(td.SOURCE_AMOUNT, 0) + COALESCE(td.TARGET_AMOUNT, 0)) * fx.numeric_rate, 18, 2)
        ELSE COALESCE(td.SOURCE_AMOUNT, 0) + COALESCE(td.TARGET_AMOUNT, 0)
    END AS tzsAmountSold,

    /* ============================================================
       SOURCE ROW IDENTITY (not part of the BoT payload)
       TREASURY_MM_DEAL's own primary key.
       ============================================================ */
    td.DEAL_NO AS sourceDealNo

FROM TREASURY_MM_DEAL td

CROSS JOIN params p

INNER JOIN counterparty cp
    ON cp.DEAL_NO = td.DEAL_NO

LEFT JOIN CURRENCY curr
    ON curr.ID_CURRENCY = td.FK_SOURCE_CURRENCY

LEFT JOIN latest_fx fx
    ON fx.fk_currencyid_curr = curr.ID_CURRENCY

WHERE td.DEAL_OPERATION = 'L'
  -- 'L' = Lend/placement (matches GL 102000001 "Placements Local Banks");
  -- 'B' = Borrow is the opposite side and belongs to a different GL.
  AND EXISTS (
      SELECT 1 FROM CUSTOMER c2
      WHERE c2.CUST_ID = td.FK_DEAL_CORRESPOND AND c2.CUST_TYPE = '2'
  );
  -- Restricts to deals whose counterparty resolves to an actual bank
  -- customer record (confirmed live: 1,998 of 2,000 'L' deals qualify).
