SELECT
    CURRENT_TIMESTAMP AS reportingDate,
    al.AGENT_ID AS agentId,
    'active' AS agentStatus,
    gte.TRN_DATE AS transactionDate,

    -- Proper transactionId construction
    VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
    TRIM(gte.FK_USRCODE) || '-' ||
    VARCHAR(gte.LINE_NUM) || '-' ||
    VARCHAR(gte.TRN_DATE) || '-' ||
    VARCHAR(gte.TRN_SNUM) AS transactionId,

    CASE
        WHEN gl.EXTERNAL_GLACCOUNT = '230000079' THEN 'Cash Deposit'
        WHEN gl.EXTERNAL_GLACCOUNT = '144000054' THEN 'Cash Withdraw'
    END AS transactionType,

    'Point of Sale' AS serviceChannel,
    NULL AS tillNumber,
    gte.CURRENCY_SHORT_DES AS currency,
    gte.DC_AMOUNT AS tzsAmount


FROM GLI_TRX_EXTRACT gte
INNER JOIN (
    SELECT DISTINCT
        CASE
            WHEN LENGTH(TRIM(TERMINAL_ID)) >= 8
            THEN SUBSTR(TRIM(TERMINAL_ID), LENGTH(TRIM(TERMINAL_ID)) - 7, 8)
            ELSE TRIM(TERMINAL_ID)
        END AS TERMINAL_ID_8,
        AGENT_ID
    FROM AGENTS_LIST
) al
    ON al.TERMINAL_ID_8 = TRIM(gte.TRX_USR)
LEFT JOIN GLG_ACCOUNT gl
    ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
WHERE gl.EXTERNAL_GLACCOUNT IN ('230000079', '144000054');
