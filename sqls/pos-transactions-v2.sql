-- POS Transactions v2 - Optimized version with improved JOIN performance
-- Changes from v1:
-- 1. Simplified JOIN condition using normalized terminal IDs
-- 2. Added WITH clause for better readability and performance
-- 3. Added proper NULL handling for optional fields

WITH normalized_agents AS (
    SELECT 
        al.AGENT_ID,
        al.TERMINAL_ID,
        -- Normalize terminal ID for matching (remove spaces, take last 8 chars if longer)
        CASE
            WHEN LENGTH(REPLACE(al.TERMINAL_ID, ' ', '')) > 8
                THEN RIGHT(REPLACE(al.TERMINAL_ID, ' ', ''), 8)
            ELSE REPLACE(al.TERMINAL_ID, ' ', '')
        END AS normalized_terminal_id
    FROM AGENTS_LIST_V4 al
),
normalized_transactions AS (
    SELECT 
        gte.*,
        -- Normalize TRX_USR for matching (remove spaces, take last 8 chars if longer)
        CASE
            WHEN LENGTH(REPLACE(gte.TRX_USR, ' ', '')) > 8
                THEN RIGHT(REPLACE(gte.TRX_USR, ' ', ''), 8)
            ELSE REPLACE(gte.TRX_USR, ' ', '')
        END AS normalized_trx_usr
    FROM GLI_TRX_EXTRACT gte
    WHERE gte.FK_GLG_ACCOUNTACCO IN ('2.3.0.00.0079', '1.4.4.00.0054')
)

SELECT 
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
    COALESCE(nt.TRX_USR, '')                          AS posNumber,
    VARCHAR_FORMAT(nt.TRN_DATE, 'DDMMYYYYHHMM')       AS transactionDate,
    VARCHAR(nt.FK_UNITCODETRXUNIT) || '-' ||
    TRIM(nt.FK_USRCODE) || '-' ||
    VARCHAR(nt.LINE_NUM) || '-' ||
    VARCHAR(nt.TRN_DATE) || '-' ||
    VARCHAR(nt.TRN_SNUM)                              AS transactionId,
    CASE nt.FK_GLG_ACCOUNTACCO
        WHEN '2.3.0.00.0079' THEN 'Cash Deposit'
        WHEN '1.4.4.00.0054' THEN 'Cash Withdraw'
        ELSE 'Unknown'
    END                                               AS transactionType,
    COALESCE(nt.CURRENCY_SHORT_DES, 'TZS')           AS currency,
    COALESCE(nt.DC_AMOUNT, 0)                        AS orgCurrencyTransactionAmount,
    COALESCE(nt.DC_AMOUNT, 0)                        AS tzsTransactionAmount,
    COALESCE(nt.DC_AMOUNT * 0.18, 0)                 AS valueAddedTaxAmount,
    0                                                 AS exciseDutyAmount,
    0                                                 AS electronicLevyAmount

FROM normalized_transactions nt
    INNER JOIN CUSTOMER c ON nt.CUST_ID = c.CUST_ID
    INNER JOIN normalized_agents na ON nt.normalized_trx_usr = na.normalized_terminal_id;