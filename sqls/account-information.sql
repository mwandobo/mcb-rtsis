SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')      AS reportingDate,

    CAST(pa.CUST_ID AS VARCHAR(20))                        AS customerIdentificationNumber,

    TRIM(pa.ACCOUNT_NUMBER)                                AS accountNumber,

    -- Product code: map overdrafts to parent current account product
    CASE
        WHEN pa.PRODUCT_ID = 40030 THEN '31705TZS'
        WHEN pa.PRODUCT_ID = 40034 THEN '31704TZS'
        WHEN pa.PRODUCT_ID = 40035 THEN '31708TZS'
        WHEN pa.PRODUCT_ID = 40037 THEN '31703TZS'
        WHEN pa.PRODUCT_ID = 40040 THEN '31730USD'
        ELSE
            CAST(pa.PRODUCT_ID AS VARCHAR(20)) ||
            TRIM(COALESCE(curr.SHORT_DESCR, 'TZS'))
    END                                                    AS accountProductCode,

    -- Account operation status mapped to BOT D64
    CASE pa.ACC_STATUS
        WHEN '1' THEN 'active'
        WHEN '6' THEN 'dormant'
        WHEN '3' THEN 'dormant'
        WHEN '2' THEN 'closed'
        WHEN '0' THEN 'inactive'
        WHEN '5' THEN 'abandoned'
        WHEN '4' THEN 'non contributing'
        ELSE          'inactive'
    END                                                    AS accountOperationStatus,

    -- Customer type mapped to BOT D01
    CASE wdc.CUST_TYPE_IND
        WHEN 'Natural'      THEN 'Individual'
        WHEN 'Corporate'    THEN 'Private company'
        WHEN 'Correspodent' THEN 'Private company'
        ELSE                     'Individual'
    END                                                    AS customerType,

    -- SMR code mapped to BOT Table 144
    CASE
        WHEN wdc.NON_RESIDENT = '1' THEN 'Not Applicable'
        WHEN wdc.CUST_TYPE_IND IS NOT NULL THEN 'Non-Government'
        ELSE 'Others'
    END                                                    AS smrCode,

    -- KYC flag: Y if account locked due to KYC (ACC_STATUS = 2)
    CASE pa.ACC_STATUS
        WHEN '2' THEN 'Y'
        ELSE          'N'
    END                                                    AS status,

    -- Original balance in account currency
    COALESCE(
        sab.BOOK_BALANCE,
        ABS(lnb.VALEUR_BALANCE),
        DECIMAL(0, 18, 2)
    )                                                      AS orgAccountBalance,

    -- USD equivalent balance
    CASE
        WHEN TRIM(curr.SHORT_DESCR) = 'USD' THEN
            DECIMAL(
                COALESCE(sab.BOOK_BALANCE, ABS(lnb.VALEUR_BALANCE), 0),
                18, 2
            )
        WHEN TRIM(curr.SHORT_DESCR) = 'TZS'
         AND usd_fx.RATE IS NOT NULL AND usd_fx.RATE <> 0 THEN
            DECIMAL(
                COALESCE(sab.BOOK_BALANCE, ABS(lnb.VALEUR_BALANCE), 0)
                / usd_fx.RATE,
                18, 2
            )
        WHEN TRIM(curr.SHORT_DESCR) NOT IN ('TZS', 'USD')
         AND fx.RATE IS NOT NULL AND fx.RATE <> 0
         AND usd_fx.RATE IS NOT NULL AND usd_fx.RATE <> 0 THEN
            DECIMAL(
                COALESCE(sab.BOOK_BALANCE, ABS(lnb.VALEUR_BALANCE), 0)
                * fx.RATE / usd_fx.RATE,
                18, 2
            )
        ELSE
            DECIMAL(
                COALESCE(sab.BOOK_BALANCE, ABS(lnb.VALEUR_BALANCE), 0),
                18, 2
            )
    END                                                    AS usdAccountBalance,

    -- TZS equivalent balance
    CASE
        WHEN TRIM(curr.SHORT_DESCR) = 'TZS' THEN
            DECIMAL(
                COALESCE(sab.BOOK_BALANCE, ABS(lnb.VALEUR_BALANCE), 0),
                18, 2
            )
        WHEN fx.RATE IS NOT NULL AND fx.RATE <> 0 THEN
            DECIMAL(
                COALESCE(sab.BOOK_BALANCE, ABS(lnb.VALEUR_BALANCE), 0)
                * fx.RATE,
                18, 2
            )
        ELSE
            DECIMAL(
                COALESCE(sab.BOOK_BALANCE, ABS(lnb.VALEUR_BALANCE), 0),
                18, 2
            )
    END                                                    AS tzsAccountBalance

FROM PROFITS_ACCOUNT pa

    -- Latest customer record only
    LEFT JOIN (
        SELECT *
        FROM (
            SELECT
                wdc.*,
                ROW_NUMBER() OVER (
                    PARTITION BY CUST_ID
                    ORDER BY CUSTOMER_BEGIN_DAT DESC
                ) AS rn
            FROM W_DIM_CUSTOMER wdc
        ) ranked
        WHERE rn = 1
    ) wdc ON wdc.CUST_ID = pa.CUST_ID

    -- Currency lookup
    LEFT JOIN (
        SELECT ID_CURRENCY, MIN(SHORT_DESCR) AS SHORT_DESCR
        FROM CURRENCY
        GROUP BY ID_CURRENCY
    ) curr ON curr.ID_CURRENCY = pa.MOVEMENT_CURRENCY

    -- Deposit balance
    LEFT JOIN STAT_ACCOUNT_BAL sab
           ON sab.ACCOUNT_NUMBER = pa.ACCOUNT_SER_NUM

    -- Loan balance — latest row per loan only
    LEFT JOIN (
        SELECT
            FK_LOAN_ACCOUNTFK,
            FK0LOAN_ACCOUNTACC,
            FK_LOAN_ACCOUNTACC,
            VALEUR_BALANCE
        FROM (
            SELECT
                lnb.*,
                ROW_NUMBER() OVER (
                    PARTITION BY FK_LOAN_ACCOUNTFK,
                                 FK0LOAN_ACCOUNTACC,
                                 FK_LOAN_ACCOUNTACC
                    ORDER BY VALUE_DATE DESC
                ) AS rn
            FROM LOAN_NRM_VAL_BAL lnb
        ) ranked
        WHERE rn = 1
    ) lnb ON lnb.FK_LOAN_ACCOUNTFK  = pa.LNS_OPEN_UNIT
         AND lnb.FK0LOAN_ACCOUNTACC  = pa.LNS_TYPE
         AND lnb.FK_LOAN_ACCOUNTACC  = pa.LNS_SN

    -- Fixing rate for account currency (TZS per 1 foreign unit)
    LEFT JOIN (
        SELECT fr.fk_currencyid_curr, fr.rate
        FROM fixing_rate fr
        WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN (
            SELECT
                fk_currencyid_curr,
                activation_date,
                MAX(activation_time)
            FROM fixing_rate
            WHERE activation_date = (
                SELECT MAX(activation_date)
                FROM fixing_rate
                WHERE activation_date <= CURRENT_DATE
            )
            GROUP BY fk_currencyid_curr, activation_date
        )
    ) fx ON fx.fk_currencyid_curr = pa.MOVEMENT_CURRENCY

    -- USD rate — for converting TZS and other currencies to USD
    LEFT JOIN (
        SELECT fr.rate
        FROM fixing_rate fr
        JOIN CURRENCY c ON c.ID_CURRENCY = fr.fk_currencyid_curr
        WHERE TRIM(c.SHORT_DESCR) = 'USD'
          AND (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN (
            SELECT
                fk_currencyid_curr,
                activation_date,
                MAX(activation_time)
            FROM fixing_rate
            WHERE activation_date = (
                SELECT MAX(activation_date)
                FROM fixing_rate
                WHERE activation_date <= CURRENT_DATE
            )
            GROUP BY fk_currencyid_curr, activation_date
        )
    ) usd_fx ON 1 = 1

WHERE TRIM(pa.ACCOUNT_NUMBER) <> 'DUMMY TRS-LG'
  AND TRIM(pa.ACCOUNT_NUMBER) <> ''
  AND pa.PRFT_SYSTEM <> 19
  AND pa.PRODUCT_ID NOT IN (38220, 38801)

ORDER BY pa.ACCOUNT_NUMBER;