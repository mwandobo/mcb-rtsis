-- Cash Statistics: cash held at tellers, by currency and denomination.
-- D26 mapping: Cash with tellers; denominations below 1,000 are Coins,
-- and denominations of 1,000 or above are Notes.
SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
    VARCHAR(u.CODE) AS branchCode,
    'Cash with tellers' AS cashCategory,
    CASE
        WHEN ctd.FK_DENOMINATION < 1000.00 THEN 'Coins'
        ELSE 'Notes'
    END AS cashSubCategory,
    TRIM(c.SHORT_DESCR) AS currency,
    ctd.FK_DENOMINATION AS cashDenomination,

    DECIMAL(SUM(ctd.ITEMS), 18, 0)
        AS quantityOfCoinsNotes,
    DECIMAL(SUM(ctd.BALANCE), 18, 2)
        AS orgAmount,

    CASE
        WHEN TRIM(c.SHORT_DESCR) = 'TZS' THEN DECIMAL(0, 18, 2)
        WHEN TRIM(c.SHORT_DESCR) = 'USD' THEN
            DECIMAL(SUM(ctd.BALANCE), 18, 2)
        WHEN fx.rate > 0 THEN
            -- DECFLOAT avoids DB2's negative-scale error during DECIMAL division.
            DECIMAL(
                DECFLOAT(SUM(ctd.BALANCE))
                / DECFLOAT(fx.rate),
                18, 2
            )
        ELSE NULL
    END AS usdAmount,

    CASE
        WHEN TRIM(c.SHORT_DESCR) = 'USD' AND fx.rate > 0 THEN
            DECIMAL(SUM(ctd.BALANCE) * fx.rate, 18, 2)
        ELSE DECIMAL(SUM(ctd.BALANCE), 18, 2)
    END AS tzsAmount
FROM CASH_TILL_DENOMIN ctd
INNER JOIN CASH_TILL ct
    ON ct.UNIT = ctd.FK_TILL_UNIT
   AND ct.FK_CURRENCYID_CURR = ctd.FK_TILL_CURRENCY
   AND ct.TILL_NO = ctd.FK_TILL_NO
INNER JOIN UNIT u
    ON u.CODE = ctd.FK_TILL_UNIT
INNER JOIN CURRENCY c
    ON c.ID_CURRENCY = ctd.FK_ID_CURRENCY
LEFT JOIN (
    SELECT fr.fk_currencyid_curr, fr.rate
    FROM fixing_rate fr
    WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN (
        SELECT fk_currencyid_curr, activation_date, MAX(activation_time)
        FROM fixing_rate
        WHERE activation_date = (
            SELECT MAX(b.activation_date)
            FROM fixing_rate b
            WHERE b.activation_date <= CURRENT_DATE
        )
        GROUP BY fk_currencyid_curr, activation_date
    )
) fx
    ON fx.fk_currencyid_curr = c.ID_CURRENCY
WHERE TRIM(c.SHORT_DESCR) <> ''
GROUP BY
    u.CODE,
    c.SHORT_DESCR,
    ctd.FK_DENOMINATION,
    ctd.FK_NOTES_COINS,
    fx.rate
HAVING SUM(ctd.ITEMS) > 0
   AND SUM(ctd.BALANCE) > 0;
