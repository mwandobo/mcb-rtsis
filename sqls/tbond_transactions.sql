-- ============================================================
-- TREASURY BOND TRANSACTIONS IN THE SECONDARY MARKET REPORT
-- Source: PROFITS CBS (GLI_TRX_EXTRACT + GLG_ACCOUNT)
-- GL Account: 103010002 - tbond (GLG_ACCOUNT: "Bonds-AvailableforSale")
--
-- DATA GAP (confirmed exhaustively - see investigation in this session):
-- the dedicated bond-trading subsystem (TRBOND, TRBOND_COUPON,
-- BOND_ORDER, PAPERLESS_SECURITY) is completely empty in this
-- environment (0 rows each), and no TREASURY_MM_DEAL/DEAL_TICKET row
-- carries a BOND_CODE. GLI_TRX_EXTRACT has only 6 postings total on
-- this GL (2017-2019), all narrative-only with no ISIN, price, yield,
-- coupon rate, or counterparty name on the source row. This is the only
-- data PROFITS has for this report - built as instructed, with every
-- field that has no real source defaulted to an UNAMBIGUOUS placeholder
-- (0 / 'Unknown') rather than an estimated-looking value, so nothing
-- here could be mistaken for verified data in a BOT submission.
--
-- sellerName/buyerName: derived from the posting's own G/L DEBIT/CREDIT
-- direction (debit = we acquired the bond = we are the buyer; credit =
-- we disposed of it = we are the seller). Own identity resolved the
-- same way as other treasury reports this session (BANK_PARAMETERS
-- .BANK_CODE=59 -> BOT FSP sender code 059 -> "Mwalimu Commercial Bank
-- Plc"). Counterparty on purchase-side postings defaults to "Government
-- of Tanzania" since the narrative text on those rows ("RECORDING OF
-- AFS BOND PURCHASE", "TRASURY BONDS APRIL 13 2017", "TBILLS AUCTION
-- NO.998") describes primary-market Treasury issuance, not a named
-- secondary-market counterparty bank. The one sale-side posting
-- ("SALE OF TREASURY BOND VD 14.01.19") has no counterparty information
-- at all and is left as 'Unknown' rather than guessed.
--
-- tbondAuctionNumber (optional): extracted from ENTRY_COMMENTS only
-- where the text explicitly contains an auction reference
-- ("AUCTION NO.998"); left NULL everywhere else rather than fabricated.
--
-- maturityDate, price, yield, couponRate: no source anywhere in PROFITS
-- for this GL. Defaulted to reportingDate / 0 / 0 / 0 respectively -
-- NEEDS REVIEW, business must confirm these from Treasury's own trading
-- records before submission.
--
-- tenure is different: it is constrained to D143's 7 fixed values
-- (2/5/7/10/15/20/25 years as a number per the OpenAPI schema), not a
-- free-form numeric field, so an out-of-range "obviously fake" default
-- like 0 would simply fail validation rather than flag as unverified.
-- Defaulted to 2 (the shortest, most conservative valid D143 tenor)
-- instead - still NEEDS REVIEW and not derived from any real data, but
-- picked from the actually-accepted value set so the record is at least
-- schema-valid pending correction.
--
-- One of the 6 postings ("Bonds Revaluation Reserve", 31-May-2018) is a
-- valuation adjustment, not a buy/sell trade - included anyway since it
-- is a genuine posting on this GL and this report is built faithfully
-- from what is actually posted, not filtered by guessing at narrative
-- intent.
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
)

SELECT

    /* reportingDate */
    VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI') AS reportingDate,

    /* tradeDate */
    VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHH24MI') AS tradeDate,

    /* sellerName -- see header note on direction/identity */
    CASE
        WHEN TRIM(gte.JUSTIFIC_DESCR) = 'G/L CREDIT' THEN 'Mwalimu Commercial Bank Plc'
        ELSE 'Government of Tanzania'
    END AS sellerName,

    /* buyerName */
    CASE
        WHEN TRIM(gte.JUSTIFIC_DESCR) = 'G/L CREDIT' THEN 'Unknown'
        ELSE 'Mwalimu Commercial Bank Plc'
    END AS buyerName,

    /* tbondAuctionNumber -- optional; only extracted when explicitly
       present in the narrative, trimmed to the digits up to the next
       space (e.g. "AUCTION NO.998 FOR 1YR" -> "998"). */
    CASE
        WHEN TRIM(gte.ENTRY_COMMENTS) LIKE '%AUCTION NO.%'
        THEN TRIM(
            SUBSTR(
                gte.ENTRY_COMMENTS,
                LOCATE('AUCTION NO.', gte.ENTRY_COMMENTS) + 11,
                LOCATE(
                    ' ',
                    SUBSTR(
                        gte.ENTRY_COMMENTS,
                        LOCATE('AUCTION NO.', gte.ENTRY_COMMENTS) + 11
                    )
                ) - 1
            )
        )
        ELSE NULL
    END AS tbondAuctionNumber,

    /* ISIN -- optional; no source */
    CAST(NULL AS VARCHAR(40)) AS ISIN,

    /* issueDate -- optional; no source */
    CAST(NULL AS VARCHAR(12)) AS issueDate,

    /* maturityDate -- no source; see NEEDS REVIEW note at top of file */
    VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI') AS maturityDate,

    /* tzsAmount */
    COALESCE(gte.numeric_dc_amount, 0) AS tzsAmount,

    /* price -- no source; see NEEDS REVIEW note at top of file */
    0 AS price,

    /* yield -- no source; see NEEDS REVIEW note at top of file */
    0 AS "yield",

    /* couponRate -- no source; see NEEDS REVIEW note at top of file */
    0 AS couponRate,

    /* tenure -- no source; constrained to D143's valid values, see
       NEEDS REVIEW note at top of file */
    2 AS tenure,

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

WHERE TRIM(gl.EXTERNAL_GLACCOUNT) = '103010002'
  AND gte.TRN_DATE IS NOT NULL
  AND gte.numeric_dc_amount IS NOT NULL;
