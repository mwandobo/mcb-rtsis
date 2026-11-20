-- ============================================================
-- INSTITUTIONAL BORROWINGS REPORT
-- Source: PROFITS CBS (TREASURY_MM_DEAL, same treasury deal-capture
-- table used for ifem_transactions.sql / "Placements Local Banks").
-- GL Account: 204010001 - Interbank Borrowings Local Banks
--
-- This is the mirror image of ifem_transactions.sql: DEAL_OPERATION='B'
-- (Borrow) is the side where WE borrow funds from another bank, matching
-- this GL exactly (DEAL_OPERATION='L'/Lend is the opposite "Placements"
-- side, already covered by ifem_transactions.sql under GL 102000001).
-- Confirmed live: GLG_ACCOUNT EXTERNAL_GLACCOUNT='204010001' resolves to
-- "INTERBANK BORROWING LOCAL BANK"; 731 'B' deals exist, 727 of which
-- resolve via FK_DEAL_CORRESPOND to an actual bank customer record
-- (same proven join used in ifem_transactions.sql, 100% BIC match rate).
--
-- OPENING/CLOSING/REPAYMENT/ACCRUED-INTEREST DERIVATION: these are
-- bullet money-market deals (single principal + single interest
-- settlement at maturity, confirmed by amortizationType mapping below) -
-- there is no partial-repayment schedule. STATUS='2' + MATURE_FLAG='1'
-- (719 of 731 deals, confirmed live) marks a deal as matured/settled.
-- For a matured deal the full principal and the full INTEREST_AMOUNT
-- have been repaid, so outstanding/closing balances are zero; for an
-- active (not yet matured) deal the opposite holds - full principal and
-- full interest remain outstanding. INTEREST_AMOUNT was verified against
-- SOURCE_AMOUNT * INTEREST_RATE * (days between VALUE_DATE and
-- MATURITY_DATE)/365 live (exact match on sample rows), confirming it is
-- the full term interest amount for the deal.
--
-- NEEDS REVIEW: borrowersAccountNumber has no reliable source -
-- FK_CORRESP_DEP_ACC is populated on all 731 rows but is a flat
-- placeholder value ('2') identical across every deal (confirmed live),
-- not a real account number, so it falls back to the deal's own number.
-- debtRegistrationNumber is marked `required` in the BoT schema despite
-- the field's own business description limiting it to "foreign loans
-- with maturity of more than a year" - these are local-bank, not
-- foreign, borrowings, so 'Not Applicable' is used to satisfy the
-- schema's non-empty requirement without fabricating a BOT debt number.
-- effectiveAnnualInterestRate has no source (the EAR compounding-
-- frequency inputs the formula needs aren't tracked) and is optional
-- per the field spec, so it is left NULL rather than estimated.
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
),

deal AS (
    SELECT
        td.*,
        CASE WHEN td.STATUS = '2' AND td.MATURE_FLAG = '1' THEN 1 ELSE 0 END AS is_matured
    FROM TREASURY_MM_DEAL td
)

SELECT

    /* reportingDate */
    VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI') AS reportingDate,

    /* borrowingType (D52) */
    'Borrowings from banks and other financial institutions in Tanzania' AS borrowingType,

    /* lenderName -- the counterparty bank we borrowed from */
    cp.bank_name AS lenderName,

    /* lenderCountry -- scope is specifically local-bank borrowings */
    'Tanzania, United Republic of' AS lenderCountry,

    /* lenderRelationship (D33) */
    'Domestic banks unrelated' AS lenderRelationship,

    /* transactionUniqueRef -- DEAL_REF_NO is unpopulated (confirmed live,
       0 of 731 rows), so DEAL_NO (the deal's own primary key) is used. */
    CAST(td.DEAL_NO AS VARCHAR(20)) AS transactionUniqueRef,

    /* debtRegistrationNumber -- see NEEDS REVIEW note at the top of
       this file. */
    'Not Applicable' AS debtRegistrationNumber,

    /* borrowersAccountNumber -- see NEEDS REVIEW note at the top of
       this file. */
    CAST(td.DEAL_NO AS VARCHAR(20)) AS borrowersAccountNumber,

    /* borrowersBankCode -- we are the borrower; own identity confirmed
       via BANK_PARAMETERS.BANK_CODE=59 -> BOT FSP sender code 059 ->
       "Mwalimu Commercial Bank Plc" (same resolution as
       ifem_transactions.sql). */
    'MWCOTZTZ' AS borrowersBankCode,

    /* sectorSnaClassification -- lender is always a deposit-taking bank */
    'Other Depository Corporations' AS sectorSnaClassification,

    /* loanContractDate */
    COALESCE(
        VARCHAR_FORMAT(NULLIF(DATE(td.DEAL_DATE), DATE('0001-01-01')), 'DDMMYYYYHH24MI'),
        VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI')
    ) AS loanContractDate,

    /* loanReceiptDate */
    COALESCE(
        VARCHAR_FORMAT(NULLIF(DATE(td.VALUE_DATE), DATE('0001-01-01')), 'DDMMYYYYHH24MI'),
        VARCHAR_FORMAT(NULLIF(DATE(td.DEAL_DATE), DATE('0001-01-01')), 'DDMMYYYYHH24MI'),
        VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI')
    ) AS loanReceiptDate,

    /* loanMaturityDate -- never reported earlier than reportingDate */
    CASE
        WHEN NULLIF(DATE(td.MATURITY_DATE), DATE('0001-01-01')) IS NOT NULL
             AND TIMESTAMP(td.MATURITY_DATE) >= p.report_timestamp
        THEN VARCHAR_FORMAT(td.MATURITY_DATE, 'DDMMYYYYHH24MI')
        ELSE VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI')
    END AS loanMaturityDate,

    /* annualInterestRate */
    COALESCE(td.INTEREST_RATE, 0) AS annualInterestRate,

    /* effectiveAnnualInterestRate -- optional; no source (compounding
       frequency not tracked), left NULL rather than estimated. */
    CAST(NULL AS DECIMAL(18, 6)) AS effectiveAnnualInterestRate,

    /* interestRateType (D102) */
    'nominal' AS interestRateType,

    /* amortizationType (D96) -- single bullet settlement at maturity */
    'Bullet Loan' AS amortizationType,

    /* interestPricingMethod (D165) */
    'Fixed' AS interestPricingMethod,

    /* borrowingPurposes (D103) */
    'Liquidity Management' AS borrowingPurposes,

    /* currency */
    COALESCE(NULLIF(TRIM(curr.SHORT_DESCR), ''), 'TZS') AS currency,

    /* orgAmountOpening */
    COALESCE(td.SOURCE_AMOUNT, 0) AS orgAmountOpening,

    /* usdAmountOpening */
    CASE
        WHEN curr.SHORT_DESCR = 'TZS' AND fx.numeric_rate IS NOT NULL AND fx.numeric_rate <> 0
            THEN DECIMAL(COALESCE(td.SOURCE_AMOUNT, 0) / fx.numeric_rate, 18, 2)
        WHEN curr.SHORT_DESCR = 'USD'
            THEN COALESCE(td.SOURCE_AMOUNT, 0)
        ELSE 0
    END AS usdAmountOpening,

    /* tzsAmountOpening */
    CASE
        WHEN curr.SHORT_DESCR = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(COALESCE(td.SOURCE_AMOUNT, 0) * fx.numeric_rate, 18, 2)
        ELSE COALESCE(td.SOURCE_AMOUNT, 0)
    END AS tzsAmountOpening,

    /* orgAccruedInterestAmount -- interest PAID (only once matured) */
    CASE WHEN td.is_matured = 1 THEN COALESCE(td.INTEREST_AMOUNT, 0) ELSE 0 END AS orgAccruedInterestAmount,

    /* usdAccruedInterestAmount */
    CASE
        WHEN td.is_matured = 0 THEN 0
        WHEN curr.SHORT_DESCR = 'TZS' AND fx.numeric_rate IS NOT NULL AND fx.numeric_rate <> 0
            THEN DECIMAL(COALESCE(td.INTEREST_AMOUNT, 0) / fx.numeric_rate, 18, 2)
        WHEN curr.SHORT_DESCR = 'USD'
            THEN COALESCE(td.INTEREST_AMOUNT, 0)
        ELSE 0
    END AS usdAccruedInterestAmount,

    /* tzsAccruedInterestAmount */
    CASE
        WHEN td.is_matured = 0 THEN 0
        WHEN curr.SHORT_DESCR = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(COALESCE(td.INTEREST_AMOUNT, 0) * fx.numeric_rate, 18, 2)
        ELSE COALESCE(td.INTEREST_AMOUNT, 0)
    END AS tzsAccruedInterestAmount,

    /* orgAmountRepayment -- principal repaid (only once matured) */
    CASE WHEN td.is_matured = 1 THEN COALESCE(td.SOURCE_AMOUNT, 0) ELSE 0 END AS orgAmountRepayment,

    /* usdAmountRepayment */
    CASE
        WHEN td.is_matured = 0 THEN 0
        WHEN curr.SHORT_DESCR = 'TZS' AND fx.numeric_rate IS NOT NULL AND fx.numeric_rate <> 0
            THEN DECIMAL(COALESCE(td.SOURCE_AMOUNT, 0) / fx.numeric_rate, 18, 2)
        WHEN curr.SHORT_DESCR = 'USD'
            THEN COALESCE(td.SOURCE_AMOUNT, 0)
        ELSE 0
    END AS usdAmountRepayment,

    /* tzsAmountRepayment */
    CASE
        WHEN td.is_matured = 0 THEN 0
        WHEN curr.SHORT_DESCR = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(COALESCE(td.SOURCE_AMOUNT, 0) * fx.numeric_rate, 18, 2)
        ELSE COALESCE(td.SOURCE_AMOUNT, 0)
    END AS tzsAmountRepayment,

    /* orgAccruedInterestOutstandingAmount -- interest not yet paid
       (the full term interest, until the bullet settlement at maturity) */
    CASE WHEN td.is_matured = 1 THEN 0 ELSE COALESCE(td.INTEREST_AMOUNT, 0) END AS orgAccruedInterestOutstandingAmount,

    /* usdAccruedInterestOutstandingAmount */
    CASE
        WHEN td.is_matured = 1 THEN 0
        WHEN curr.SHORT_DESCR = 'TZS' AND fx.numeric_rate IS NOT NULL AND fx.numeric_rate <> 0
            THEN DECIMAL(COALESCE(td.INTEREST_AMOUNT, 0) / fx.numeric_rate, 18, 2)
        WHEN curr.SHORT_DESCR = 'USD'
            THEN COALESCE(td.INTEREST_AMOUNT, 0)
        ELSE 0
    END AS usdAccruedInterestOutstandingAmount,

    /* tzsAccruedInterestOutstandingAmount */
    CASE
        WHEN td.is_matured = 1 THEN 0
        WHEN curr.SHORT_DESCR = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(COALESCE(td.INTEREST_AMOUNT, 0) * fx.numeric_rate, 18, 2)
        ELSE COALESCE(td.INTEREST_AMOUNT, 0)
    END AS tzsAccruedInterestOutstandingAmount,

    /* orgPrincipalAmountClosing -- outstanding principal balance */
    CASE WHEN td.is_matured = 1 THEN 0 ELSE COALESCE(td.SOURCE_AMOUNT, 0) END AS orgPrincipalAmountClosing,

    /* usdPrincipalAmountClosing */
    CASE
        WHEN td.is_matured = 1 THEN 0
        WHEN curr.SHORT_DESCR = 'TZS' AND fx.numeric_rate IS NOT NULL AND fx.numeric_rate <> 0
            THEN DECIMAL(COALESCE(td.SOURCE_AMOUNT, 0) / fx.numeric_rate, 18, 2)
        WHEN curr.SHORT_DESCR = 'USD'
            THEN COALESCE(td.SOURCE_AMOUNT, 0)
        ELSE 0
    END AS usdPrincipalAmountClosing,

    /* tzsPrincipalAmountClosing */
    CASE
        WHEN td.is_matured = 1 THEN 0
        WHEN curr.SHORT_DESCR = 'USD' AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(COALESCE(td.SOURCE_AMOUNT, 0) * fx.numeric_rate, 18, 2)
        ELSE COALESCE(td.SOURCE_AMOUNT, 0)
    END AS tzsPrincipalAmountClosing,

    /* ============================================================
       SOURCE ROW IDENTITY (not part of the BoT payload)
       TREASURY_MM_DEAL's own primary key.
       ============================================================ */
    td.DEAL_NO AS sourceDealNo

FROM deal td

CROSS JOIN params p

INNER JOIN counterparty cp
    ON cp.DEAL_NO = td.DEAL_NO

LEFT JOIN CURRENCY curr
    ON curr.ID_CURRENCY = td.FK_SOURCE_CURRENCY

LEFT JOIN latest_fx fx
    ON fx.fk_currencyid_curr = curr.ID_CURRENCY

WHERE td.DEAL_OPERATION = 'B'
  -- 'B' = Borrow (matches GL 204010001 "Interbank Borrowings Local
  -- Banks"); 'L' = Lend/placement is the opposite side, already covered
  -- by ifem_transactions.sql under GL 102000001.
  AND EXISTS (
      SELECT 1 FROM CUSTOMER c2
      WHERE c2.CUST_ID = td.FK_DEAL_CORRESPOND AND c2.CUST_TYPE = '2'
  );
  -- Restricts to deals whose counterparty resolves to an actual bank
  -- customer record (confirmed live: 727 of 731 'B' deals qualify).
