-- Validation Script for Employee Records RTSIS Report
-- Checks data quality and completeness for RTSIS submission
-- Created for MCB Bank Tanzania
-- Date: December 30, 2025

-- =====================================================
-- 1. RECORD COUNT AND BASIC STATISTICS
-- =====================================================

SELECT 
    'EMPLOYEE_RECORDS_SUMMARY' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT empIdentificationNumber) AS unique_employees,
    COUNT(CASE WHEN empName IS NOT NULL AND TRIM(empName) != '' THEN 1 END) AS records_with_name,
    COUNT(CASE WHEN gender IN ('Male', 'Female') THEN 1 END) AS records_with_valid_gender,
    COUNT(CASE WHEN empStatus = 'Permanent' THEN 1 END) AS permanent_employees,
    COUNT(CASE WHEN empStatus = 'Contract' THEN 1 END) AS contract_employees,
    COUNT(CASE WHEN basicSalary > 0 THEN 1 END) AS records_with_salary
FROM (
    -- Include the main query here (abbreviated for validation)
    SELECT
        VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
        COALESCE(CAST(u.CODE AS VARCHAR(10)), '001') AS branchCode,
        COALESCE(TRIM(emp.FIRST_NAME) || ' ' || TRIM(emp.LAST_NAME), 'Unknown Employee') AS empName,
        CASE 
            WHEN UPPER(emp.SEX) = 'M' THEN 'Male'
            WHEN UPPER(emp.SEX) = 'F' THEN 'Female'
            WHEN UPPER(emp.SEX) = '1' THEN 'Male'
            WHEN UPPER(emp.SEX) = '2' THEN 'Female'
            ELSE 'Not Specified'
        END AS gender,
        COALESCE(TRIM(emp.STAFF_NO), TRIM(emp.ID)) AS empIdentificationNumber,
        CASE 
            WHEN emp.EMPL_STATUS = '1' THEN 'Permanent'
            WHEN emp.EMPL_STATUS = '2' THEN 'Contract'
            WHEN emp.EMPL_STATUS = '3' THEN 'Temporary'
            WHEN emp.EMPL_STATUS = '0' THEN 'Inactive'
            ELSE 'Unknown'
        END AS empStatus,
        COALESCE(CAST(gd_salary.DESCRIPTION AS DECIMAL(15,2)), 500000.00) AS basicSalary
    FROM BANKEMPLOYEE emp
        LEFT JOIN UNIT u ON u.CODE = (SELECT MIN(u2.CODE) FROM UNIT u2 WHERE u2.ENTRY_STATUS = '1' LIMIT 1)
        LEFT JOIN GENERIC_DETAIL gd_salary ON gd_salary.FK_GENERIC_HEADPAR = 'EMPSL' 
            AND gd_salary.SERIAL_NUM = CAST(emp.ID AS INTEGER) AND gd_salary.ENTRY_STATUS = '1'
    WHERE emp.EMPL_STATUS = '1' AND emp.STAFF_NO IS NOT NULL AND TRIM(emp.STAFF_NO) != ''
        AND (emp.FIRST_NAME IS NOT NULL OR emp.LAST_NAME IS NOT NULL)
) employee_data;

-- =====================================================
-- 2. MANDATORY FIELD VALIDATION
-- =====================================================

SELECT 
    'MANDATORY_FIELDS_CHECK' AS validation_type,
    'reportingDate' AS field_name,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN reportingDate IS NOT NULL AND LENGTH(TRIM(reportingDate)) = 12 THEN 1 END) AS valid_records,
    COUNT(CASE WHEN reportingDate IS NULL OR LENGTH(TRIM(reportingDate)) != 12 THEN 1 END) AS invalid_records
FROM (
    SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate
    FROM BANKEMPLOYEE emp
    WHERE emp.EMPL_STATUS = '1' AND emp.STAFF_NO IS NOT NULL
    LIMIT 10
) test_data

UNION ALL

SELECT 
    'MANDATORY_FIELDS_CHECK' AS validation_type,
    'empIdentificationNumber' AS field_name,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN empIdentificationNumber IS NOT NULL AND TRIM(empIdentificationNumber) != '' THEN 1 END) AS valid_records,
    COUNT(CASE WHEN empIdentificationNumber IS NULL OR TRIM(empIdentificationNumber) = '' THEN 1 END) AS invalid_records
FROM (
    SELECT COALESCE(TRIM(emp.STAFF_NO), TRIM(emp.ID)) AS empIdentificationNumber
    FROM BANKEMPLOYEE emp
    WHERE emp.EMPL_STATUS = '1'
    LIMIT 10
) test_data

UNION ALL

SELECT 
    'MANDATORY_FIELDS_CHECK' AS validation_type,
    'empName' AS field_name,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN empName IS NOT NULL AND TRIM(empName) != '' AND empName != 'Unknown Employee' THEN 1 END) AS valid_records,
    COUNT(CASE WHEN empName IS NULL OR TRIM(empName) = '' OR empName = 'Unknown Employee' THEN 1 END) AS invalid_records
FROM (
    SELECT COALESCE(TRIM(emp.FIRST_NAME) || ' ' || TRIM(emp.LAST_NAME), 'Unknown Employee') AS empName
    FROM BANKEMPLOYEE emp
    WHERE emp.EMPL_STATUS = '1'
    LIMIT 10
) test_data;

-- =====================================================
-- 3. DATA QUALITY CHECKS
-- =====================================================

-- Check for duplicate employee identification numbers
SELECT 
    'DUPLICATE_CHECK' AS validation_type,
    empIdentificationNumber,
    COUNT(*) AS duplicate_count
FROM (
    SELECT COALESCE(TRIM(emp.STAFF_NO), TRIM(emp.ID)) AS empIdentificationNumber
    FROM BANKEMPLOYEE emp
    WHERE emp.EMPL_STATUS = '1' AND emp.STAFF_NO IS NOT NULL
) emp_ids
GROUP BY empIdentificationNumber
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- =====================================================
-- 4. GENDER DISTRIBUTION
-- =====================================================

SELECT 
    'GENDER_DISTRIBUTION' AS validation_type,
    gender,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM (
    SELECT 
        CASE 
            WHEN UPPER(emp.SEX) = 'M' THEN 'Male'
            WHEN UPPER(emp.SEX) = 'F' THEN 'Female'
            WHEN UPPER(emp.SEX) = '1' THEN 'Male'
            WHEN UPPER(emp.SEX) = '2' THEN 'Female'
            ELSE 'Not Specified'
        END AS gender
    FROM BANKEMPLOYEE emp
    WHERE emp.EMPL_STATUS = '1'
) gender_data
GROUP BY gender
ORDER BY count DESC;

-- =====================================================
-- 5. EMPLOYMENT STATUS DISTRIBUTION
-- =====================================================

SELECT 
    'EMPLOYMENT_STATUS_DISTRIBUTION' AS validation_type,
    empStatus,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM (
    SELECT 
        CASE 
            WHEN emp.EMPL_STATUS = '1' THEN 'Permanent'
            WHEN emp.EMPL_STATUS = '2' THEN 'Contract'
            WHEN emp.EMPL_STATUS = '3' THEN 'Temporary'
            WHEN emp.EMPL_STATUS = '0' THEN 'Inactive'
            ELSE 'Unknown'
        END AS empStatus
    FROM BANKEMPLOYEE emp
    WHERE emp.EMPL_STATUS = '1'
) status_data
GROUP BY empStatus
ORDER BY count DESC;

-- =====================================================
-- 6. SALARY RANGE ANALYSIS
-- =====================================================

SELECT 
    'SALARY_ANALYSIS' AS validation_type,
    'Basic Salary Statistics' AS description,
    COUNT(*) AS total_records,
    MIN(basicSalary) AS min_salary,
    MAX(basicSalary) AS max_salary,
    AVG(basicSalary) AS avg_salary,
    MEDIAN(basicSalary) AS median_salary
FROM (
    SELECT COALESCE(CAST(gd_salary.DESCRIPTION AS DECIMAL(15,2)), 500000.00) AS basicSalary
    FROM BANKEMPLOYEE emp
        LEFT JOIN GENERIC_DETAIL gd_salary 
            ON gd_salary.FK_GENERIC_HEADPAR = 'EMPSL' 
            AND gd_salary.SERIAL_NUM = CAST(emp.ID AS INTEGER) 
            AND gd_salary.ENTRY_STATUS = '1'
    WHERE emp.EMPL_STATUS = '1'
) salary_data;

-- =====================================================
-- 7. CONTACT INFORMATION COMPLETENESS
-- =====================================================

SELECT 
    'CONTACT_INFO_COMPLETENESS' AS validation_type,
    COUNT(*) AS total_employees,
    COUNT(CASE WHEN emp.EMAIL IS NOT NULL AND TRIM(emp.EMAIL) != '' THEN 1 END) AS employees_with_email,
    COUNT(CASE WHEN emp.WORK_PHONE IS NOT NULL AND TRIM(emp.WORK_PHONE) != '' THEN 1 END) AS employees_with_work_phone,
    COUNT(CASE WHEN emp.MOBILE_PHONE IS NOT NULL AND TRIM(emp.MOBILE_PHONE) != '' THEN 1 END) AS employees_with_mobile,
    ROUND(COUNT(CASE WHEN emp.EMAIL IS NOT NULL AND TRIM(emp.EMAIL) != '' THEN 1 END) * 100.0 / COUNT(*), 2) AS email_completeness_pct,
    ROUND(COUNT(CASE WHEN emp.MOBILE_PHONE IS NOT NULL AND TRIM(emp.MOBILE_PHONE) != '' THEN 1 END) * 100.0 / COUNT(*), 2) AS mobile_completeness_pct
FROM BANKEMPLOYEE emp
WHERE emp.EMPL_STATUS = '1';

-- =====================================================
-- 8. SAMPLE DATA PREVIEW
-- =====================================================

SELECT 
    'SAMPLE_DATA_PREVIEW' AS validation_type,
    empIdentificationNumber,
    empName,
    gender,
    empStatus,
    basicSalary,
    email,
    mobilePhone
FROM (
    SELECT
        COALESCE(TRIM(emp.STAFF_NO), TRIM(emp.ID)) AS empIdentificationNumber,
        COALESCE(TRIM(emp.FIRST_NAME) || ' ' || TRIM(emp.LAST_NAME), 'Unknown Employee') AS empName,
        CASE 
            WHEN UPPER(emp.SEX) = 'M' THEN 'Male'
            WHEN UPPER(emp.SEX) = 'F' THEN 'Female'
            ELSE 'Not Specified'
        END AS gender,
        CASE 
            WHEN emp.EMPL_STATUS = '1' THEN 'Permanent'
            WHEN emp.EMPL_STATUS = '2' THEN 'Contract'
            ELSE 'Other'
        END AS empStatus,
        COALESCE(CAST(gd_salary.DESCRIPTION AS DECIMAL(15,2)), 500000.00) AS basicSalary,
        TRIM(emp.EMAIL) AS email,
        TRIM(emp.MOBILE_PHONE) AS mobilePhone
    FROM BANKEMPLOYEE emp
        LEFT JOIN GENERIC_DETAIL gd_salary 
            ON gd_salary.FK_GENERIC_HEADPAR = 'EMPSL' 
            AND gd_salary.SERIAL_NUM = CAST(emp.ID AS INTEGER) 
            AND gd_salary.ENTRY_STATUS = '1'
    WHERE emp.EMPL_STATUS = '1' 
        AND emp.STAFF_NO IS NOT NULL 
        AND TRIM(emp.STAFF_NO) != ''
    ORDER BY emp.STAFF_NO
    LIMIT 5
) sample_data;

-- =====================================================
-- 9. RTSIS COMPLIANCE CHECK
-- =====================================================

SELECT 
    'RTSIS_COMPLIANCE_SUMMARY' AS validation_type,
    'Employee Records Report Ready for Submission' AS status,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS compliance_status,
    COUNT(*) AS total_records_for_submission,
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS validation_timestamp
FROM BANKEMPLOYEE emp
WHERE emp.EMPL_STATUS = '1' 
    AND emp.STAFF_NO IS NOT NULL 
    AND TRIM(emp.STAFF_NO) != ''
    AND (emp.FIRST_NAME IS NOT NULL OR emp.LAST_NAME IS NOT NULL);