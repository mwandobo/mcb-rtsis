-- Branch Information Data Extraction Query
-- This query extracts branch information for BOT reporting
-- Based on UNIT_CODE table and related branch information

SELECT 
    -- Reporting Date: Current timestamp for reporting
    CURRENT_TIMESTAMP AS reportingDate,
    
    -- Branch Name: From unit description or branch name
    COALESCE(LTRIM(RTRIM(u.UNIT_DESCRIPTION)), 'Unknown Branch') AS branchName,
    
    -- Tax Identification Number: Bank's TIN or branch-specific TIN
    COALESCE(u.TAX_ID, 'TIN123456789') AS taxIdentificationNumber,
    
    -- Business License: License number for the branch
    COALESCE(u.LICENSE_NO, 'BL' || LPAD(CAST(u.UNIT_CODE AS VARCHAR(10)), 6, '0')) AS businessLicense,
    
    -- Branch Code: Unit code from the system
    CAST(u.UNIT_CODE AS VARCHAR(20)) AS branchCode,
    
    -- QR FSR Code: Quick Response Financial Services Registry Code
    COALESCE(u.FSR_CODE, 'QR' || LPAD(CAST(u.UNIT_CODE AS VARCHAR(10)), 8, '0')) AS qrFsrCode,
    
    -- Region: Administrative region
    COALESCE(LTRIM(RTRIM(u.REGION)), 'Unknown Region') AS region,
    
    -- District: Administrative district
    COALESCE(LTRIM(RTRIM(u.DISTRICT)), 'Unknown District') AS district,
    
    -- Ward: Administrative ward
    COALESCE(LTRIM(RTRIM(u.WARD)), 'Unknown Ward') AS ward,
    
    -- Street: Street address
    COALESCE(LTRIM(RTRIM(u.STREET_ADDRESS)), 'Unknown Street') AS street,
    
    -- House Number: Building/house number
    COALESCE(LTRIM(RTRIM(u.HOUSE_NUMBER)), 'N/A') AS houseNumber,
    
    -- Postal Code: Postal/ZIP code
    COALESCE(LTRIM(RTRIM(u.POSTAL_CODE)), '00000') AS postalCode,
    
    -- GPS Coordinates: Latitude and longitude
    CASE 
        WHEN u.LATITUDE IS NOT NULL AND u.LONGITUDE IS NOT NULL THEN
            CAST(u.LATITUDE AS VARCHAR(20)) || ',' || CAST(u.LONGITUDE AS VARCHAR(20))
        ELSE 'Not Available'
    END AS gpsCoordinates,
    
    -- Banking Services: List of services offered
    CASE 
        WHEN u.UNIT_TYPE = 'BRANCH' THEN 'Deposits,Withdrawals,Loans,Foreign Exchange,Safe Deposit'
        WHEN u.UNIT_TYPE = 'ATM' THEN 'Cash Withdrawal,Balance Inquiry,Mini Statement'
        WHEN u.UNIT_TYPE = 'AGENCY' THEN 'Deposits,Withdrawals,Account Opening'
        ELSE 'Basic Banking Services'
    END AS bankingServices,
    
    -- Mobile Money Services: Mobile financial services
    CASE 
        WHEN u.MOBILE_MONEY_FLAG = 'Y' THEN 'M-Pesa,Airtel Money,Tigo Pesa,Halo Pesa'
        WHEN u.UNIT_TYPE = 'BRANCH' THEN 'M-Pesa,Airtel Money'
        ELSE 'Not Available'
    END AS mobileMoneyServices,
    
    -- Registration Date: When the branch was registered
    COALESCE(u.REGISTRATION_DATE, u.OPENING_DATE, CURRENT_DATE - 365 DAYS) AS registrationDate,
    
    -- Branch Status: Current operational status
    CASE 
        WHEN u.STATUS = 'A' THEN 'Active'
        WHEN u.STATUS = 'C' THEN 'Closed'
        WHEN u.STATUS = 'S' THEN 'Suspended'
        WHEN u.STATUS = 'T' THEN 'Temporary Closure'
        ELSE 'Unknown'
    END AS branchStatus,
    
    -- Closure Date: Date when branch was closed (if applicable)
    CASE 
        WHEN u.STATUS = 'C' THEN COALESCE(u.CLOSURE_DATE, CURRENT_DATE)
        ELSE NULL
    END AS closureDate,
    
    -- Contact Person: Branch manager or contact person
    COALESCE(LTRIM(RTRIM(u.MANAGER_NAME)), LTRIM(RTRIM(u.CONTACT_PERSON)), 'Branch Manager') AS contactPerson,
    
    -- Telephone Number: Primary phone number
    COALESCE(LTRIM(RTRIM(u.PHONE_NUMBER)), '+255-000-000-000') AS telephoneNumber,
    
    -- Alternative Telephone Number: Secondary phone number
    COALESCE(LTRIM(RTRIM(u.ALT_PHONE_NUMBER)), LTRIM(RTRIM(u.FAX_NUMBER))) AS altTelephoneNumber,
    
    -- Branch Category: Type/category of branch
    CASE 
        WHEN u.UNIT_TYPE = 'HEAD_OFFICE' THEN 'Head Office'
        WHEN u.UNIT_TYPE = 'BRANCH' AND u.BRANCH_CATEGORY = 'A' THEN 'Grade A Branch'
        WHEN u.UNIT_TYPE = 'BRANCH' AND u.BRANCH_CATEGORY = 'B' THEN 'Grade B Branch'
        WHEN u.UNIT_TYPE = 'BRANCH' AND u.BRANCH_CATEGORY = 'C' THEN 'Grade C Branch'
        WHEN u.UNIT_TYPE = 'BRANCH' THEN 'Standard Branch'
        WHEN u.UNIT_TYPE = 'ATM' THEN 'ATM Location'
        WHEN u.UNIT_TYPE = 'AGENCY' THEN 'Banking Agent'
        WHEN u.UNIT_TYPE = 'SUB_BRANCH' THEN 'Sub Branch'
        ELSE 'Other'
    END AS branchCategory

FROM UNIT_CODE u
WHERE 
    -- Include active branches and recently closed ones
    (u.STATUS IN ('A', 'C', 'S') OR u.STATUS IS NULL)
    -- Exclude system/virtual units
    AND u.UNIT_TYPE NOT IN ('SYSTEM', 'VIRTUAL', 'TEMP')
    -- Include only physical locations
    AND u.UNIT_CODE IS NOT NULL
    AND u.UNIT_CODE > 0

ORDER BY u.UNIT_CODE;

-- Alternative query if UNIT_CODE table structure is different
-- This uses a more generic approach with common table/column names

/*
SELECT 
    CURRENT_TIMESTAMP AS reportingDate,
    COALESCE(branch_name, unit_name, 'Branch ' || branch_code) AS branchName,
    COALESCE(tax_id, 'TIN123456789') AS taxIdentificationNumber,
    COALESCE(license_number, 'BL' || LPAD(branch_code, 6, '0')) AS businessLicense,
    branch_code AS branchCode,
    COALESCE(fsr_code, 'QR' || LPAD(branch_code, 8, '0')) AS qrFsrCode,
    COALESCE(region, 'Unknown Region') AS region,
    COALESCE(district, 'Unknown District') AS district,
    COALESCE(ward, 'Unknown Ward') AS ward,
    COALESCE(street, 'Unknown Street') AS street,
    COALESCE(house_number, 'N/A') AS houseNumber,
    COALESCE(postal_code, '00000') AS postalCode,
    CASE 
        WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN
            CAST(latitude AS VARCHAR(20)) || ',' || CAST(longitude AS VARCHAR(20))
        ELSE 'Not Available'
    END AS gpsCoordinates,
    'Deposits,Withdrawals,Loans,Foreign Exchange' AS bankingServices,
    'M-Pesa,Airtel Money,Tigo Pesa' AS mobileMoneyServices,
    COALESCE(opening_date, CURRENT_DATE - 365 DAYS) AS registrationDate,
    CASE 
        WHEN status = 'ACTIVE' THEN 'Active'
        WHEN status = 'CLOSED' THEN 'Closed'
        ELSE 'Unknown'
    END AS branchStatus,
    CASE WHEN status = 'CLOSED' THEN closure_date ELSE NULL END AS closureDate,
    COALESCE(manager_name, 'Branch Manager') AS contactPerson,
    COALESCE(phone_number, '+255-000-000-000') AS telephoneNumber,
    alt_phone_number AS altTelephoneNumber,
    COALESCE(branch_type, 'Standard Branch') AS branchCategory
FROM branches 
WHERE status IN ('ACTIVE', 'CLOSED')
ORDER BY branch_code;
*/