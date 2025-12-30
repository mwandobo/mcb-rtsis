-- Test Script for Employee Data
-- Quick check of BANKEMPLOYEE table structure and sample data
-- Created for MCB Bank Tanzania
-- Date: December 30, 2025

-- =====================================================
-- 1. TABLE STRUCTURE CHECK
-- =====================================================

-- Check if BANKEMPLOYEE table exists and get basic info
SELECT 
    'TABLE_INFO' AS check_type,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN EMPL_STATUS = '1' THEN 1 END) AS active_employees,
    COUNT(CASE WHEN EMPL_STATUS = '0' THEN 1 END) AS inactive_employees,
    COUNT(DISTINCT STAFF_NO) AS unique_staff_numbers,
    COUNT(CASE WHEN STAFF_NO IS NOT NULL THEN 1 END) AS records_with_staff_no
FROM BANKEMPLOYEE;

-- =====================================================
-- 2. SAMPLE RECORDS
-- =====================================================

-- Get first 5 employee records to understand data structure
SELECT 
    'SAMPLE_RECORDS' AS check_type,
    ID,
    STAFF_NO,
    FIRST_NAME,
    LAST_NAME,
    SEX,
    EMPL_STATUS,
    EMAIL,
    WORK_PHONE,
    MOBILE_PHONE,
    SIGNATURE_LEVEL,
    FATHER_NAME
FROM BANKEMPLOYEE
WHERE STAFF_NO IS NOT NULL
ORDER BY STAFF_NO
LIMIT 5;

-- =====================================================
-- 3. DATA QUALITY CHECKS
-- =====================================================

-- Check for missing critical fields
SELECT 
    'DATA_QUALITY' AS check_type,
    'Missing Names' AS issue_type,
    COUNT(*) AS count
FROM BANKEMPLOYEE
WHERE (FIRST_NAME IS NULL OR TRIM(FIRST_NAME) = '') 
    AND (LAST_NAME IS NULL OR TRIM(LAST_NAME) = '')
    AND EMPL_STATUS = '1'

UNION ALL

SELECT 
    'DATA_QUALITY' AS check_type,
    'Missing Staff Numbers' AS issue_type,
    COUNT(*) AS count
FROM BANKEMPLOYEE
WHERE (STAFF_NO IS NULL OR TRIM(STAFF_NO) = '')
    AND EMPL_STATUS = '1'

UNION ALL

SELECT 
    'DATA_QUALITY' AS check_type,
    'Missing Gender' AS issue_type,
    COUNT(*) AS count
FROM BANKEMPLOYEE
WHERE (SEX IS NULL OR TRIM(SEX) = '')
    AND EMPL_STATUS = '1';

-- =====================================================
-- 4. EMPLOYMENT STATUS BREAKDOWN
-- =====================================================

SELECT 
    'EMPLOYMENT_STATUS' AS check_type,
    EMPL_STATUS,
    COUNT(*) AS count,
    CASE 
        WHEN EMPL_STATUS = '1' THEN 'Active'
        WHEN EMPL_STATUS = '0' THEN 'Inactive'
        WHEN EMPL_STATUS = '2' THEN 'Contract'
        WHEN EMPL_STATUS = '3' THEN 'Temporary'
        ELSE 'Unknown'
    END AS status_description
FROM BANKEMPLOYEE
GROUP BY EMPL_STATUS
ORDER BY count DESC;

-- =====================================================
-- 5. GENDER DISTRIBUTION
-- =====================================================

SELECT 
    'GENDER_DISTRIBUTION' AS check_type,
    SEX,
    COUNT(*) AS count,
    CASE 
        WHEN UPPER(SEX) = 'M' THEN 'Male'
        WHEN UPPER(SEX) = 'F' THEN 'Female'
        WHEN SEX = '1' THEN 'Male'
        WHEN SEX = '2' THEN 'Female'
        ELSE 'Unknown'
    END AS gender_description
FROM BANKEMPLOYEE
WHERE EMPL_STATUS = '1'
GROUP BY SEX
ORDER BY count DESC;

-- =====================================================
-- 6. SIGNATURE LEVEL DISTRIBUTION
-- =====================================================

SELECT 
    'SIGNATURE_LEVELS' AS check_type,
    SIGNATURE_LEVEL,
    COUNT(*) AS count,
    CASE 
        WHEN SIGNATURE_LEVEL IN ('A', 'B', 'C') THEN 'Senior Management'
        WHEN SIGNATURE_LEVEL IN ('D', 'E') THEN 'Middle Management'
        WHEN SIGNATURE_LEVEL IN ('F', 'G', 'H') THEN 'Junior Staff'
        ELSE 'Unclassified'
    END AS level_description
FROM BANKEMPLOYEE
WHERE EMPL_STATUS = '1'
GROUP BY SIGNATURE_LEVEL
ORDER BY count DESC;

-- =====================================================
-- 7. CONTACT INFORMATION AVAILABILITY
-- =====================================================

SELECT 
    'CONTACT_INFO' AS check_type,
    COUNT(*) AS total_active_employees,
    COUNT(CASE WHEN EMAIL IS NOT NULL AND TRIM(EMAIL) != '' THEN 1 END) AS employees_with_email,
    COUNT(CASE WHEN WORK_PHONE IS NOT NULL AND TRIM(WORK_PHONE) != '' THEN 1 END) AS employees_with_work_phone,
    COUNT(CASE WHEN MOBILE_PHONE IS NOT NULL AND TRIM(MOBILE_PHONE) != '' THEN 1 END) AS employees_with_mobile_phone
FROM BANKEMPLOYEE
WHERE EMPL_STATUS = '1';

-- =====================================================
-- 8. FOREIGN KEY RELATIONSHIPS
-- =====================================================

SELECT 
    'FOREIGN_KEYS' AS check_type,
    COUNT(DISTINCT FKGH_HAS_AS_GRADE) AS unique_grades,
    COUNT(DISTINCT FKGH_WORKS_IN_POSI) AS unique_positions,
    COUNT(CASE WHEN FKGH_HAS_AS_GRADE IS NOT NULL THEN 1 END) AS employees_with_grade,
    COUNT(CASE WHEN FKGH_WORKS_IN_POSI IS NOT NULL THEN 1 END) AS employees_with_position
FROM BANKEMPLOYEE
WHERE EMPL_STATUS = '1';

-- =====================================================
-- 9. RECENT RECORDS CHECK
-- =====================================================

SELECT 
    'RECENT_RECORDS' AS check_type,
    COUNT(*) AS records_last_30_days,
    MAX(TMSTAMP) AS latest_timestamp,
    MIN(TMSTAMP) AS earliest_timestamp
FROM BANKEMPLOYEE
WHERE TMSTAMP >= CURRENT_DATE - 30 DAYS;

-- =====================================================
-- 10. SIMPLE RTSIS QUERY TEST
-- =====================================================

-- Test a simplified version of the main RTSIS query
SELECT 
    'RTSIS_TEST' AS check_type,
    COALESCE(TRIM(STAFF_NO), TRIM(ID)) AS empIdentificationNumber,
    COALESCE(TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME), 'Unknown') AS empName,
    CASE 
        WHEN UPPER(SEX) = 'M' THEN 'Male'
        WHEN UPPER(SEX) = 'F' THEN 'Female'
        ELSE 'Not Specified'
    END AS gender,
    CASE 
        WHEN EMPL_STATUS = '1' THEN 'Permanent'
        WHEN EMPL_STATUS = '2' THEN 'Contract'
        ELSE 'Other'
    END AS empStatus,
    TRIM(EMAIL) AS email
FROM BANKEMPLOYEE
WHERE EMPL_STATUS = '1' 
    AND STAFF_NO IS NOT NULL 
    AND TRIM(STAFF_NO) != ''
ORDER BY STAFF_NO
LIMIT 3;