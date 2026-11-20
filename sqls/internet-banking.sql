-- GL 2.3.0.00.0062 = "ATM payable". Per internal personnel, the subset that
-- represents internet-banking-initiated (cardless) cash withdrawals is
-- TRX_CODE 3191 / ENTRY_COMMENTS = 'ATM DEBIT'. TRX_CODE 3311 rows on the
-- same GL are reversals/cancellations of those withdrawals (mirror-negative
-- amount, same account/date), confirmed by inspecting raw rows.
--
-- There is no explicit "original transaction reference" column linking a
-- cancellation back to the debit it reverses, so pairing is done by
-- matching (ACCOUNT_NUMBER, TRN_DATE, amount) and pairing off the nth debit
-- against the nth cancellation in that group ordered by TRN_SNUM. This is a
-- best-effort match, not a guaranteed exact correspondence - if it matters,
-- validate against a period where cancellations are rare enough to spot-check.
WITH
    debits AS (
        SELECT
            gte.*,
            ROW_NUMBER() OVER (
                PARTITION BY
                    gte.ACCOUNT_NUMBER,
                    gte.TRN_DATE,
                    gte.DC_AMOUNT
                ORDER BY
                    gte.TRN_SNUM
            ) AS pair_rn
        FROM
            GLI_TRX_EXTRACT gte
        WHERE
            gte.FK_GLG_ACCOUNTACCO = '2.3.0.00.0062'
            AND gte.TRX_CODE = 3191
    ),
    cancels AS (
        SELECT
            gte.ACCOUNT_NUMBER,
            gte.TRN_DATE,
            - gte.DC_AMOUNT AS DC_AMOUNT,
            ROW_NUMBER() OVER (
                PARTITION BY
                    gte.ACCOUNT_NUMBER,
                    gte.TRN_DATE,
                    - gte.DC_AMOUNT
                ORDER BY
                    gte.TRN_SNUM
            ) AS pair_rn
        FROM
            GLI_TRX_EXTRACT gte
        WHERE
            gte.FK_GLG_ACCOUNTACCO = '2.3.0.00.0062'
            AND gte.TRX_CODE = 3311
    ),
    completed_withdrawals AS (
        -- Debits with no matching cancellation in the same account/date/amount
        -- bucket survive; matched (cancelled) pairs are dropped from both sides.
        SELECT
            d.*
        FROM
            debits d
            LEFT JOIN cancels c ON c.ACCOUNT_NUMBER = d.ACCOUNT_NUMBER
            AND c.TRN_DATE = d.TRN_DATE
            AND c.DC_AMOUNT = d.DC_AMOUNT
            AND c.pair_rn = d.pair_rn
        WHERE
            c.pair_rn IS NULL
    )
SELECT
    VARCHAR_FORMAT (CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
    VARCHAR_FORMAT (gte.TRN_DATE, 'DDMMYYYYHHMM') AS transactionDate,
    NULLIF(TRIM(pa.ACCOUNT_NUMBER), '') AS accountNumber,
    gte.CUST_ID AS customerIdentificationNumber,
    COALESCE(
        NULLIF(
            TRIM(cust.FIRST_NAME) || CASE
                WHEN NULLIF(TRIM(cust.MIDDLE_NAME), '') IS NOT NULL THEN ' ' || TRIM(cust.MIDDLE_NAME)
                ELSE ''
            END || CASE
                WHEN NULLIF(TRIM(cust.SURNAME), '') IS NOT NULL THEN ' ' || TRIM(cust.SURNAME)
                ELSE ''
            END,
            ''
        ),
        NULLIF(TRIM(cust.SHORT_NAME), '')
    ) AS beneficiaryName,
    'TANZANIA, UNITED REPUBLIC OF' AS beneficiaryCountry,
    'MWCOTZTZ' AS benBankOrWalletCode,
    NULLIF(TRIM(pa.ACCOUNT_NUMBER), '') AS benAccountOrMobileNumber,
    'Internet banking' AS serviceCategory,
    'Others Services' AS subServiceCategory,
    -- Table D64 values: active, inactive, dormant, closed, abandoned, non contributing.
    -- Defaulted to 'active' because a GL posting implies the service was
    -- transacting at that moment; not derived from an actual status lookup.
    'active' AS serviceStatus,
    VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' || TRIM(gte.FK_USRCODE) || '-' || VARCHAR(gte.LINE_NUM) || '-' || VARCHAR(gte.TRN_DATE) || '-' || VARCHAR(gte.TRN_SNUM) AS transactionRef,
    gte.CURRENCY_SHORT_DES AS currency,
    -- Original amount (as transacted)
    DECIMAL(gte.DC_AMOUNT, 18, 2) AS orgAmount,
    -- Amount converted to USD
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
        WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
        ELSE DECIMAL(gte.DC_AMOUNT / NULLIF(fx.RATE, 0), 18, 2)
    END AS usdAmount,
    -- Amount converted to TZS
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
        WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN DECIMAL(gte.DC_AMOUNT * fx.RATE, 18, 2)
        ELSE NULL
    END AS tzsAmount,
    DECIMAL(gte.DC_AMOUNT * 0.18, 18, 2) AS valueAddedTaxAmount,
    0 AS exciseDutyAmount,
    0 AS electronicLevyAmount
FROM
    completed_withdrawals gte
    LEFT JOIN (
        SELECT
            *
        FROM
            (
                SELECT
                    pa.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            CUST_ID
                        ORDER BY
                            ACCOUNT_NUMBER
                    ) rn
                FROM
                    PROFITS_ACCOUNT pa
                WHERE
                    PRFT_SYSTEM = 3
            )
        WHERE
            rn = 1
    ) pa ON pa.CUST_ID = gte.CUST_ID
    LEFT JOIN CUSTOMER cust ON cust.CUST_ID = gte.CUST_ID
    -- Join Currency Using SHORT_DESCR
    -- =========================================
    LEFT JOIN CURRENCY curr ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES
    -- =============================================
    -- Latest Fixing Rate Per Currency
    -- =============================================
    LEFT JOIN (
        SELECT
            fr.fk_currencyid_curr,
            fr.rate
        FROM
            fixing_rate fr
        WHERE
            (
                fr.fk_currencyid_curr,
                fr.activation_date,
                fr.activation_time
            ) IN (
                SELECT
                    fk_currencyid_curr,
                    activation_date,
                    MAX(activation_time)
                FROM
                    fixing_rate
                WHERE
                    activation_date = (
                        SELECT
                            MAX(b.activation_date)
                        FROM
                            fixing_rate b
                        WHERE
                            b.activation_date <= CURRENT_DATE
                    )
                GROUP BY
                    fk_currencyid_curr,
                    activation_date
            )
    ) fx ON fx.fk_currencyid_curr = curr.ID_CURRENCY;