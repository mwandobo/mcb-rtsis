SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')       AS reportingDate,

    -- Append currency to product code for uniqueness
    CAST(wp.PRODUCT_CODE AS VARCHAR(20)) || '' ||
    CASE
        WHEN UPPER(wp.DESCRIPTION) LIKE '%USD%'  THEN 'USD'
        WHEN UPPER(wp.DESCRIPTION) LIKE '%EURO%' THEN 'EUR'
        WHEN UPPER(wp.DESCRIPTION) LIKE '%GBP%'  THEN 'GBP'
        WHEN UPPER(wp.DESCRIPTION) LIKE '%EUR%'  THEN 'EUR'
        ELSE COALESCE(curr.SHORT_DESCR, 'TZS')
    END                                                      AS accountProductCode,

    TRIM(wp.DESCRIPTION)                                     AS accountProductName,
    TRIM(wp.DESCRIPTION)                                     AS accountProductDescription,

    -- accountProductType mapped to BOT D25
    CASE wp.TREE_LEVEL_1
        WHEN 'Deposit Account' THEN
            CASE wp.TREE_LEVEL_2
                WHEN 'SAVINGS ACCOUNT'      THEN 'Saving'
                WHEN 'CURRENT ACCOUNT'      THEN 'Current'
                WHEN 'OVERDRAFT'            THEN 'Current'
                WHEN 'TERM DEPOSIT ACCOUNT' THEN 'Fixed deposits'
                WHEN 'NOTICE ACCOUNT'       THEN 'Call deposits'
                ELSE 'Others'
            END
        ELSE 'Others'
    END                                                      AS accountProductType,

    -- accountProductSubType: only Current accounts have subtypes in D25
    CASE wp.TREE_LEVEL_2
        WHEN 'CURRENT ACCOUNT' THEN
            CASE
                WHEN UPPER(wp.DESCRIPTION) LIKE '%MAXCOM%'
                  OR UPPER(wp.DESCRIPTION) LIKE '%WAKALA%'
                  OR UPPER(wp.DESCRIPTION) LIKE '%AGENCY%'
                    THEN 'Mobile money Trust accounts'
                WHEN UPPER(wp.DESCRIPTION) LIKE '%INTEREST%'
                    THEN 'Mobile money interest account'
                ELSE 'Normal'
            END
        WHEN 'OVERDRAFT' THEN
            CASE
                WHEN UPPER(wp.DESCRIPTION) LIKE '%MOBILE%'
                  OR UPPER(wp.DESCRIPTION) LIKE '%WAKALA%'
                    THEN 'Mobile money Trust accounts'
                ELSE 'Normal'
            END
        ELSE NULL
    END                                                      AS accountProductSubType,

    -- Currency from product name, fallback to join
    CASE
        WHEN UPPER(wp.DESCRIPTION) LIKE '%USD%'  THEN 'USD'
        WHEN UPPER(wp.DESCRIPTION) LIKE '%EURO%' THEN 'EUR'
        WHEN UPPER(wp.DESCRIPTION) LIKE '%GBP%'  THEN 'GBP'
        WHEN UPPER(wp.DESCRIPTION) LIKE '%EUR%'  THEN 'EUR'
        ELSE COALESCE(curr.SHORT_DESCR, 'TZS')
    END                                                      AS currency,

    -- Creation date
    COALESCE(
        VARCHAR_FORMAT(
            NULLIF(p.VALIDITY_DATE, DATE('0001-01-01')),
            'DDMMYYYYHHMM'
        ),
        VARCHAR_FORMAT(CURRENT_DATE, 'DDMMYYYYHHMM')
    )                                                        AS accountProductCreationDate,

    -- Closure date — exclude all system defaults, only show when Closed
    CASE
        WHEN p.ENTRY_STATUS <> '1' THEN
            CASE
                WHEN p.PRD_EXP_DT NOT IN (
                    DATE('0001-01-01'),
                    DATE('9999-12-31'),
                    DATE('3000-12-31'),
                    DATE('2001-01-01')
                ) THEN VARCHAR_FORMAT(p.PRD_EXP_DT, 'DDMMYYYYHHMM')
                WHEN p.TMSTAMP IS NOT NULL
                    THEN VARCHAR_FORMAT(p.TMSTAMP, 'DDMMYYYYHHMM')
                ELSE NULL
            END
        ELSE NULL
    END                                                      AS accountProductClosureDate,

    -- Status mapped to BOT D171
    CASE p.ENTRY_STATUS
        WHEN '1' THEN 'Active'
        ELSE 'Closed'
    END                                                      AS accountProductStatus

FROM W_DIM_PRODUCT wp
    LEFT JOIN PRODUCT p
           ON p.ID_PRODUCT = wp.PRODUCT_CODE
    LEFT JOIN (
        SELECT DISTINCT
            pa.PRFT_SYSTEM,
            MIN(pa.MOVEMENT_CURRENCY) AS MOVEMENT_CURRENCY
        FROM PROFITS_ACCOUNT pa
        GROUP BY pa.PRFT_SYSTEM
    ) pa ON pa.PRFT_SYSTEM = wp.PRODUCT_CODE
    LEFT JOIN (
        SELECT ID_CURRENCY, MIN(SHORT_DESCR) AS SHORT_DESCR
        FROM CURRENCY
        GROUP BY ID_CURRENCY
    ) curr ON curr.ID_CURRENCY = pa.MOVEMENT_CURRENCY

WHERE wp.TREE_LEVEL_1 NOT IN ('Agreement', 'Service', 'Collateral')
  AND wp.TREE_LEVEL_2 NOT IN ('DUMMY', 'HIDDEN ACCOUNT')

ORDER BY wp.TREE_LEVEL_1, wp.PRODUCT_CODE