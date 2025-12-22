-- claimTreasury Validation Test Query
-- This query tests the structure and validates the data extraction logic

-- Test 1: Check if TREASURY_MM_DEAL has government-related records
SELECT 
    'TREASURY_MM_DEAL Record Count' AS test_description,
    COUNT(*) AS record_count
FROM TREASURY_MM_DEAL tmd
    LEFT JOIN CUSTOMER c ON c.CUST_ID = tmd.FK_CORRESP_CUST
WHERE 
    (
        -- Customer-based filtering
        UPPER(COALESCE(c.SURNAME, '')) LIKE '%GOVERNMENT%' OR
        UPPER(COALESCE(c.SURNAME, '')) LIKE '%MINISTRY%' OR
        UPPER(COALESCE(c.SURNAME, '')) LIKE '%TREASURY%' OR
        UPPER(COALESCE(c.SURNAME, '')) LIKE '%COUNCIL%' OR
        UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%GOVERNMENT%' OR
        UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%MINISTRY%' OR
        UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%TREASURY%' OR
        UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%COUNCIL%' OR
        
        -- Deal reference-based filtering
        UPPER(COALESCE(tmd.DEAL_REF_NO, '')) LIKE '%TREASURY%' OR
        UPPER(COALESCE(tmd.DEAL_REF_NO, '')) LIKE '%GOVERNMENT%' OR
        UPPER(COALESCE(tmd.DEAL_REF_NO, '')) LIKE '%MINISTRY%' OR
        
        -- Bond code-based filtering
        UPPER(COALESCE(tmd.BOND_CODE, '')) LIKE '%TREASURY%' OR
        UPPER(COALESCE(tmd.BOND_CODE, '')) LIKE '%GOVT%' OR
        UPPER(COALESCE(tmd.BOND_CODE, '')) LIKE '%GOV%'
    )
    AND tmd.STATUS = '1'
    AND COALESCE(tmd.SOURCE_AMOUNT, 0) > 0

UNION ALL

-- Test 2: Sample data structure validation (first 3 records)
SELECT 
    'Sample Data Validation' AS test_description,
    CAST(COUNT(*) AS VARCHAR(20)) AS record_count
FROM (
    SELECT 
        tmd.DEAL_NO,
        tmd.DEAL_DATE,
        tmd.SOURCE_AMOUNT,
        tmd.MATURITY_DATE,
        c.SURNAME,
        c.FIRST_NAME,
        tmd.DEAL_REF_NO,
        tmd.BOND_CODE
    FROM TREASURY_MM_DEAL tmd
        LEFT JOIN CUSTOMER c ON c.CUST_ID = tmd.FK_CORRESP_CUST
    WHERE tmd.STATUS = '1' 
        AND COALESCE(tmd.SOURCE_AMOUNT, 0) > 0
    FETCH FIRST 3 ROWS ONLY
) sample_data

UNION ALL

-- Test 3: Currency distribution check
SELECT 
    'Currency Types Available' AS test_description,
    CAST(COUNT(DISTINCT cu.SHORT_DESCR) AS VARCHAR(20)) AS record_count
FROM TREASURY_MM_DEAL tmd
    LEFT JOIN CURRENCY cu ON cu.ID_CURRENCY = tmd.FK_SOURCE_CURRENCY
WHERE tmd.STATUS = '1' 
    AND COALESCE(tmd.SOURCE_AMOUNT, 0) > 0;