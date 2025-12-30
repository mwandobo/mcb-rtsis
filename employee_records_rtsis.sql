-- Employee Records RTSIS Report
-- Based on RTSIS requirements for employee information reporting
-- Created for MCB Bank Tanzania
-- Date: December 30, 2025

SELECT
    /* =========================
       REPORTING INFORMATION
       ========================= */
    
    -- Reporting Date and Time (DDMMYYYYHHMM format)
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')       AS reportingDate,
    
    /* =========================
       BRANCH IDENTIFICATION
       ========================= */
    
    -- Branch Code (from employee's assigned unit)
    COALESCE(
        CAST(u.CODE AS VARCHAR(10)),
        '001'  -- Default head office code
    )                                                        AS branchCode,
    
    /* =========================
       EMPLOYEE IDENTIFICATION
       ========================= */
    
    -- Employee Name
    COALESCE(
        TRIM(emp.FIRST_NAME) || ' ' || TRIM(emp.LAST_NAME),
        TRIM(emp.FIRST_NAME),
        TRIM(emp.LAST_NAME),
        'Unknown Employee'
    )                                                        AS empName,
    
    -- Gender (D93 lookup)
    CASE 
        WHEN UPPER(emp.SEX) = 'M' THEN 'Male'
        WHEN UPPER(emp.SEX) = 'F' THEN 'Female'
        WHEN UPPER(emp.SEX) = '1' THEN 'Male'
        WHEN UPPER(emp.SEX) = '2' THEN 'Female'
        ELSE 'Not Specified'
    END                                                      AS gender,
    
    -- Employee Date of Birth (DDMMYYYYHHMM format)
    COALESCE(
        gd_dob.DESCRIPTION,  -- Date stored as string in DESCRIPTION
        '01011980000000'  -- Default DOB if not available
    )                                                        AS empDob,
    
    -- Employee ID Type (D02 lookup)
    COALESCE(
        gd_id_type.LATIN_DESC,
        gd_id_type.DESCRIPTION,
        'National ID'  -- Default ID type
    )                                                        AS empIdentificationType,
    
    -- Employee ID Number (Primary Key)
    COALESCE(
        TRIM(emp.STAFF_NO),
        TRIM(emp.ID)
    )                                                        AS empIdentificationNumber,
    
    /* =========================
       POSITION INFORMATION
       ========================= */
    
    -- Employee Position
    COALESCE(
        gd_position.LATIN_DESC,
        gd_position.DESCRIPTION,
        'Bank Officer'  -- Default position
    )                                                        AS empPosition,
    
    -- Employee Position Category (D173 lookup)
    COALESCE(
        gd_pos_category.LATIN_DESC,
        CASE 
            WHEN emp.SIGNATURE_LEVEL IN ('A', 'B', 'C') THEN 'Senior Management'
            WHEN emp.SIGNATURE_LEVEL IN ('D', 'E') THEN 'Middle Management'
            ELSE 'Non-Senior Management'
        END
    )                                                        AS empPositionCategory,
    
    -- Employee Status (D152 lookup)
    CASE 
        WHEN emp.EMPL_STATUS = '1' THEN 'Permanent'
        WHEN emp.EMPL_STATUS = '2' THEN 'Contract'
        WHEN emp.EMPL_STATUS = '3' THEN 'Temporary'
        WHEN emp.EMPL_STATUS = '0' THEN 'Inactive'
        ELSE 'Unknown'
    END                                                      AS empStatus,
    
    -- Employee Department
    COALESCE(
        gd_dept.LATIN_DESC,
        gd_dept.DESCRIPTION,
        u.UNIT_NAME,  -- Use branch name as department if no specific department
        'General Banking'  -- Default department
    )                                                        AS empDepartment,
    
    /* =========================
       EMPLOYMENT DATES
       ========================= */
    
    -- Appointment Date (DDMMYYYYHHMM format)
    COALESCE(
        gd_appointment.DESCRIPTION,  -- Date stored as string in DESCRIPTION
        VARCHAR_FORMAT(emp.TMSTAMP, 'DDMMYYYYHHMM'),
        '01012020000000'  -- Default appointment date
    )                                                        AS appointmentDate,
    
    -- Employee Nationality (Country lookup)
    COALESCE(
        gd_nationality.LATIN_DESC,
        gd_nationality.DESCRIPTION,
        'Tanzanian'  -- Default nationality
    )                                                        AS empNationality,
    
    -- Last Promotion Date (DDMMYYYYHHMM format)
    COALESCE(
        gd_promotion.DESCRIPTION,  -- Date stored as string in DESCRIPTION
        VARCHAR_FORMAT(emp.TMSTAMP, 'DDMMYYYYHHMM')  -- Use record timestamp as fallback
    )                                                        AS lastPromotionDate,
    
    /* =========================
       COMPENSATION INFORMATION
       ========================= */
    
    -- Basic Salary
    COALESCE(
        CAST(gd_salary.DESCRIPTION AS DECIMAL(15,2)),  -- Salary stored as string in DESCRIPTION
        500000.00  -- Default basic salary in TZS
    )                                                        AS basicSalary,
    
    -- Employee Benefits (Array - D153 lookup)
    COALESCE(
        gd_benefits.DESCRIPTION,  -- Benefits stored as string in DESCRIPTION
        'Medical Insurance, Transport Allowance, Housing Allowance'
    )                                                        AS empBenefits,
    
    /* =========================
       CONTACT INFORMATION
       ========================= */
    
    -- Email
    TRIM(emp.EMAIL)                                          AS email,
    
    -- Work Phone
    TRIM(emp.WORK_PHONE)                                     AS workPhone,
    
    -- Mobile Phone
    TRIM(emp.MOBILE_PHONE)                                   AS mobilePhone,
    
    /* =========================
       ADDITIONAL INFORMATION FOR TRACKING
       ========================= */
    
    -- Employee ID (Internal)
    emp.ID                                                   AS employeeId,
    
    -- Staff Number
    emp.STAFF_NO                                             AS staffNumber,
    
    -- Father's Name
    TRIM(emp.FATHER_NAME)                                    AS fatherName,
    
    -- Card ID
    emp.CARD_ID                                              AS cardId,
    
    -- Signature Level
    emp.SIGNATURE_LEVEL                                      AS signatureLevel,
    
    -- Grade Information
    emp.FKGH_HAS_AS_GRADE                                    AS gradeCode,
    
    -- Position Code
    emp.FKGH_WORKS_IN_POSI                                   AS positionCode,
    
    -- Record Timestamp
    emp.TMSTAMP                                              AS recordTimestamp

FROM BANKEMPLOYEE emp
    
    /* ===== UNIT/BRANCH INFORMATION ===== */
    LEFT JOIN UNIT u
        ON u.CODE = (
            -- Try to find employee's assigned branch
            -- This might need adjustment based on actual relationship
            SELECT MIN(u2.CODE) 
            FROM UNIT u2 
            WHERE u2.ENTRY_STATUS = '1'
            LIMIT 1
        )
    
    /* ===== DATE OF BIRTH LOOKUP ===== */
    LEFT JOIN GENERIC_DETAIL gd_dob
        ON gd_dob.FK_GENERIC_HEADPAR = 'EMPDB'  -- Employee Date of Birth
        AND gd_dob.SERIAL_NUM = CAST(emp.ID AS INTEGER)
        AND gd_dob.ENTRY_STATUS = '1'
    
    /* ===== ID TYPE LOOKUP (D02) ===== */
    LEFT JOIN GENERIC_DETAIL gd_id_type
        ON gd_id_type.FK_GENERIC_HEADPAR = 'D02'  -- ID Type lookup
        AND gd_id_type.SERIAL_NUM = 1  -- Default to National ID
        AND gd_id_type.ENTRY_STATUS = '1'
    
    /* ===== POSITION LOOKUP ===== */
    LEFT JOIN GENERIC_DETAIL gd_position
        ON gd_position.FK_GENERIC_HEADPAR = emp.FKGH_WORKS_IN_POSI
        AND gd_position.SERIAL_NUM = emp.FKGD_WORKS_IN_POSI
        AND gd_position.ENTRY_STATUS = '1'
    
    /* ===== POSITION CATEGORY LOOKUP (D173) ===== */
    LEFT JOIN GENERIC_DETAIL gd_pos_category
        ON gd_pos_category.FK_GENERIC_HEADPAR = 'D173'  -- Position Category
        AND gd_pos_category.SERIAL_NUM = CASE 
            WHEN emp.SIGNATURE_LEVEL IN ('A', 'B', 'C') THEN 1  -- Senior Management
            WHEN emp.SIGNATURE_LEVEL IN ('D', 'E') THEN 2  -- Middle Management
            ELSE 3  -- Non-Senior Management
        END
        AND gd_pos_category.ENTRY_STATUS = '1'
    
    /* ===== DEPARTMENT LOOKUP ===== */
    LEFT JOIN GENERIC_DETAIL gd_dept
        ON gd_dept.FK_GENERIC_HEADPAR = 'DEPT'  -- Department lookup
        AND gd_dept.SERIAL_NUM = 1  -- Default department
        AND gd_dept.ENTRY_STATUS = '1'
    
    /* ===== APPOINTMENT DATE LOOKUP ===== */
    LEFT JOIN GENERIC_DETAIL gd_appointment
        ON gd_appointment.FK_GENERIC_HEADPAR = 'EMPAP'  -- Employee Appointment
        AND gd_appointment.SERIAL_NUM = CAST(emp.ID AS INTEGER)
        AND gd_appointment.ENTRY_STATUS = '1'
    
    /* ===== NATIONALITY LOOKUP ===== */
    LEFT JOIN GENERIC_DETAIL gd_nationality
        ON gd_nationality.FK_GENERIC_HEADPAR = 'CNTRY'  -- Country/Nationality
        AND gd_nationality.SERIAL_NUM = 1  -- Default to Tanzania
        AND gd_nationality.ENTRY_STATUS = '1'
    
    /* ===== PROMOTION DATE LOOKUP ===== */
    LEFT JOIN GENERIC_DETAIL gd_promotion
        ON gd_promotion.FK_GENERIC_HEADPAR = 'EMPPR'  -- Employee Promotion
        AND gd_promotion.SERIAL_NUM = CAST(emp.ID AS INTEGER)
        AND gd_promotion.ENTRY_STATUS = '1'
    
    /* ===== SALARY LOOKUP ===== */
    LEFT JOIN GENERIC_DETAIL gd_salary
        ON gd_salary.FK_GENERIC_HEADPAR = 'EMPSL'  -- Employee Salary
        AND gd_salary.SERIAL_NUM = CAST(emp.ID AS INTEGER)
        AND gd_salary.ENTRY_STATUS = '1'
    
    /* ===== BENEFITS LOOKUP (D153) ===== */
    LEFT JOIN GENERIC_DETAIL gd_benefits
        ON gd_benefits.FK_GENERIC_HEADPAR = 'D153'  -- Employee Benefits
        AND gd_benefits.SERIAL_NUM = 1  -- Default benefits package
        AND gd_benefits.ENTRY_STATUS = '1'

WHERE 
    -- Only active employees
    emp.EMPL_STATUS = '1'
    -- Filter out test/dummy records
    AND emp.STAFF_NO IS NOT NULL
    AND TRIM(emp.STAFF_NO) != ''
    -- Ensure we have at least first name or last name
    AND (emp.FIRST_NAME IS NOT NULL OR emp.LAST_NAME IS NOT NULL)

ORDER BY 
    emp.STAFF_NO,
    emp.ID;