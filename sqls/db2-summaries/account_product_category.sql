-- Account Product Category Summary Query
-- Returns total count of account product category records
SELECT COUNT(*) as record_count
FROM (
    SELECT
        CAST(wp.PRODUCT_CODE AS VARCHAR(20)) ||
        CASE
            WHEN UPPER(wp.DESCRIPTION) LIKE '%USD%'  THEN 'USD'
            WHEN UPPER(wp.DESCRIPTION) LIKE '%EURO%' THEN 'EUR'
            WHEN UPPER(wp.DESCRIPTION) LIKE '%GBP%'  THEN 'GBP'
            WHEN UPPER(wp.DESCRIPTION) LIKE '%EUR%'  THEN 'EUR'
            ELSE COALESCE(curr.SHORT_DESCR, 'TZS')
        END AS accountProductCode
    FROM W_DIM_PRODUCT wp
        LEFT JOIN PRODUCT p ON p.ID_PRODUCT = wp.PRODUCT_CODE
        LEFT JOIN (
            SELECT DISTINCT pa.PRFT_SYSTEM,
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
)
