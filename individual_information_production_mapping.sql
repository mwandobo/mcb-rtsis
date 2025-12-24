-- Individual Information Production Mapping
-- Maps API fields directly from PROFITS source tables
-- Based on comprehensive analysis of customer view structure

SELECT 
    -- REPORTING INFORMATION
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
    CAST(c.CUST_ID AS VARCHAR(50)) AS customerIdentificationNumber,
    
    -- PERSONAL PARTICULARS
    TRIM(c.FIRST_NAME) AS firstName,
    TRIM(c.MIDDLE_NAME) AS middleNames,
    CAST(NULL AS VARCHAR(100)) AS otherNames,  -- Not available in source
    TRIM(
        COALESCE(TRIM(c.FIRST_NAME), '') || 
        CASE WHEN TRIM(c.MIDDLE_NAME) IS NOT NULL AND TRIM(c.MIDDLE_NAME) != '' 
             THEN ' ' || TRIM(c.MIDDLE_NAME) ELSE '' END ||
        CASE WHEN TRIM(c.SURNAME) IS NOT NULL AND TRIM(c.SURNAME) != '' 
             THEN ' ' || TRIM(c.SURNAME) ELSE '' END
    ) AS fullNames,
    TRIM(c.SURNAME) AS presentSurname,
    TRIM(c.MOTHER_SURNAME) AS birthSurname,
    CASE 
        WHEN UPPER(TRIM(c.SEX)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(c.SEX)) = 'F' THEN 'Female'
        ELSE 'NotSpecified'
    END AS gender,
    COALESCE(TRIM(family_cat.DESCRIPTION), 'NotSpecified') AS maritalStatus,
    CAST(NULL AS INTEGER) AS numberSpouse,  -- Not available
    COALESCE(TRIM(national_cat.DESCRIPTION), 'Unknown') AS nationality,
    COALESCE(TRIM(citizen_cat.DESCRIPTION), 'Unknown') AS citizenship,
    CASE 
        WHEN c.NON_RESIDENT = '0' THEN 'Resident'
        WHEN c.NON_RESIDENT = '1' THEN 'Non-Resident'
        ELSE 'Unknown'
    END AS residency,
    COALESCE(TRIM(profes_cat.DESCRIPTION), 'NotSpecified') AS profession,
    COALESCE(TRIM(indcode_cat.DESCRIPTION), 'NotSpecified') AS sectorSnaClassification,
    CASE 
        WHEN c.ENTRY_STATUS = '0' THEN 'Deceased'
        WHEN c.ENTRY_STATUS = '1' THEN 'Active'
        ELSE 'Unknown'
    END AS fateStatus,
    COALESCE(TRIM(proflevl_cat.DESCRIPTION), 'NotSpecified') AS socialStatus,
    COALESCE(TRIM(profcat_cat.DESCRIPTION), 'NotSpecified') AS employmentStatus,
    COALESCE(c.SALARY_AMN, 0) AS monthlyIncome,
    COALESCE(c.NUM_OF_CHILDREN, 0) AS numberDependants,
    COALESCE(TRIM(edulevel_cat.DESCRIPTION), 'NotSpecified') AS educationLevel,
    CAST(NULL AS DECIMAL(15,2)) AS averageMonthlyExpenditure,  -- Not available
    CASE 
        WHEN c.BLACKLISTED_IND = '1' THEN 'Blacklisted'
        WHEN TRIM(c.AML_STATUS) IS NOT NULL AND TRIM(c.AML_STATUS) != '' THEN TRIM(c.AML_STATUS)
        ELSE 'Clean'
    END AS negativeClientStatus,
    
    -- SPOUSE INFORMATION
    TRIM(c.SPOUSE_NAME) AS spousesFullName,
    CAST(NULL AS VARCHAR(50)) AS spouseIdentificationType,  -- Not available
    CAST(NULL AS VARCHAR(50)) AS spouseIdentificationNumber,  -- Not available
    TRIM(c.MOTHER_SURNAME) AS maidenName,  -- Using mother surname as alternative
    CAST(NULL AS DECIMAL(15,2)) AS monthlyExpenses,  -- Not available
    
    -- BIRTH INFORMATION
    CASE 
        WHEN c.DATE_OF_BIRTH IS NOT NULL AND c.DATE_OF_BIRTH > DATE('1900-01-01')
        THEN VARCHAR_FORMAT(c.DATE_OF_BIRTH, 'DDMMYYYYHHMM')
        ELSE NULL
    END AS birthDate,
    COALESCE(TRIM(bcountry_cat.DESCRIPTION), 'Unknown') AS birthCountry,
    CAST(NULL AS VARCHAR(20)) AS birthPostalCode,  -- Not available
    CAST(NULL AS VARCHAR(20)) AS birthHouseNumber,  -- Not available
    COALESCE(TRIM(region_cat.DESCRIPTION), 'Unknown') AS birthRegion,
    CAST(NULL AS VARCHAR(100)) AS birthDistrict,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS birthWard,  -- Not available
    TRIM(c.BIRTHPLACE) AS birthStreet,
    
    -- IDENTIFICATION INFORMATION
    COALESCE(TRIM(id_type.DESCRIPTION), 'Unknown') AS identificationType,
    TRIM(oid.ID_NO) AS identificationNumber,
    CASE 
        WHEN oid.ISSUE_DATE IS NOT NULL AND oid.ISSUE_DATE > DATE('1900-01-01')
        THEN VARCHAR_FORMAT(oid.ISSUE_DATE, 'DDMMYYYYHHMM')
        ELSE NULL
    END AS issuanceDate,
    CASE 
        WHEN oid.EXPIRY_DATE IS NOT NULL AND oid.EXPIRY_DATE > DATE('1900-01-01')
        THEN VARCHAR_FORMAT(oid.EXPIRY_DATE, 'DDMMYYYYHHMM')
        ELSE NULL
    END AS expirationDate,
    COALESCE(TRIM(id_country.DESCRIPTION), 'Unknown') AS issuancePlace,
    CAST(NULL AS VARCHAR(200)) AS issuingAuthority,  -- Not available
    
    -- BUSINESS INFORMATION
    CASE 
        WHEN c.CUST_TYPE = '2' THEN TRIM(c.SURNAME)  -- Company name for corporate
        ELSE NULL
    END AS businessName,
    CASE 
        WHEN c.CUSTOMER_BEGIN_DAT IS NOT NULL AND c.CUSTOMER_BEGIN_DAT > DATE('1900-01-01')
        THEN VARCHAR_FORMAT(c.CUSTOMER_BEGIN_DAT, 'DDMMYYYYHHMM')
        ELSE NULL
    END AS establishmentDate,
    TRIM(c.CHAMBER_ID) AS businessRegistrationNumber,
    CASE 
        WHEN c.CERTIFIC_DATE IS NOT NULL AND c.CERTIFIC_DATE > DATE('1900-01-01')
        THEN VARCHAR_FORMAT(c.CERTIFIC_DATE, 'DDMMYYYYHHMM')
        ELSE NULL
    END AS businessRegistrationDate,
    CAST(NULL AS VARCHAR(50)) AS businessLicenseNumber,  -- Not available
    TRIM(afm.AFM_NO) AS taxIdentificationNumber,
    
    -- EMPLOYER INFORMATION
    TRIM(c.EMPLOYER) AS employerName,
    CAST(NULL AS VARCHAR(100)) AS employerRegion,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS employerDistrict,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS employerWard,  -- Not available
    TRIM(c.EMPLOYER_ADDRESS) AS employerStreet,
    CAST(NULL AS VARCHAR(20)) AS employerHouseNumber,  -- Not available
    CAST(NULL AS VARCHAR(20)) AS employerPostalCode,  -- Not available
    COALESCE(TRIM(activity_cat.DESCRIPTION), 'NotSpecified') AS businessNature,
    
    -- INDIVIDUAL CONTACTS
    TRIM(c.MOBILE_TEL) AS mobileNumber,
    TRIM(c.MOBILE_TEL2) AS alternativeMobileNumber,
    TRIM(c.TELEPHONE_1) AS fixedLineNumber,
    TRIM(addr_comm.FAX_NO) AS faxNumber,
    TRIM(c.E_MAIL) AS emailAddress,
    TRIM(c.INTERNET_ADDRESS) AS socialMedia,
    TRIM(addr_comm.ADDRESS_1) AS mainAddress,
    
    -- PRIMARY ADDRESS (Communication Address)
    TRIM(addr_comm.ADDRESS_1) AS street,
    CAST(NULL AS VARCHAR(20)) AS houseNumber,  -- Not available
    TRIM(addr_comm.ZIP_CODE) AS postalCode,
    TRIM(addr_comm.REGION) AS region,
    CAST(NULL AS VARCHAR(100)) AS district,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS ward,  -- Not available
    COALESCE(TRIM(comm_country.DESCRIPTION), 'Tanzania') AS country,
    
    -- SECONDARY ADDRESS (Work Address)
    TRIM(addr_work.ADDRESS_1) AS secondaryStreet,
    CAST(NULL AS VARCHAR(20)) AS secondaryHouseNumber,  -- Not available
    TRIM(addr_work.ZIP_CODE) AS secondaryPostalCode,
    TRIM(addr_work.REGION) AS secondaryRegion,
    CAST(NULL AS VARCHAR(100)) AS secondaryDistrict,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS secondaryWard,  -- Not available
    COALESCE(TRIM(work_country.DESCRIPTION), 'Tanzania') AS secondaryCountry,
    
    -- METADATA FOR TRACKING
    c.CUST_TYPE AS customerType,
    c.ENTRY_STATUS AS entryStatus,
    c.LAST_UPDATE AS lastUpdate

FROM CUSTOMER c

-- Customer Categories (using optimized subqueries)
LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'FAMILY'
) family_cat ON family_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'NATIONAL'
) national_cat ON national_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'CITIZEN'
) citizen_cat ON citizen_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'PROFES'
) profes_cat ON profes_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'INDCODE'
) indcode_cat ON indcode_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'PROFLEVL'
) proflevl_cat ON proflevl_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'PROFCAT'
) profcat_cat ON profcat_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'EDULEVEL'
) edulevel_cat ON edulevel_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'BCOUNTRY'
) bcountry_cat ON bcountry_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'REGION'
) region_cat ON region_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'ACTIVITY'
) activity_cat ON activity_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

-- Identification Information (get main ID only)
LEFT JOIN OTHER_ID oid ON oid.FK_CUSTOMERCUST_ID = c.CUST_ID 
                      AND COALESCE(oid.MAIN_FLAG, '1') = '1'
LEFT JOIN GENERIC_DETAIL id_type ON id_type.FK_GENERIC_HEADPAR = oid.FKGH_HAS_TYPE
                                AND id_type.SERIAL_NUM = oid.FKGD_HAS_TYPE
LEFT JOIN GENERIC_DETAIL id_country ON id_country.FK_GENERIC_HEADPAR = oid.FKGH_HAS_BEEN_ISSU
                                   AND id_country.SERIAL_NUM = oid.FKGD_HAS_BEEN_ISSU

-- Tax Information (get main AFM only)
LEFT JOIN OTHER_AFM afm ON afm.FK_CUSTOMERCUST_ID = c.CUST_ID 
                       AND COALESCE(afm.MAIN_FLAG, '1') = '1'

-- Address Information
-- Communication Address (primary)
LEFT JOIN CUST_ADDRESS addr_comm ON addr_comm.FK_CUSTOMERCUST_ID = c.CUST_ID
                                AND addr_comm.COMMUNICATION_ADDR = '1'
                                AND addr_comm.ENTRY_STATUS = '1'
LEFT JOIN GENERIC_DETAIL comm_country ON comm_country.FK_GENERIC_HEADPAR = addr_comm.FKGH_HAS_COUNTRY
                                     AND comm_country.SERIAL_NUM = addr_comm.FKGD_HAS_COUNTRY

-- Work Address (secondary)
LEFT JOIN (
    SELECT FK_CUSTOMERCUST_ID, ADDRESS_1, ZIP_CODE, REGION, FKGH_HAS_COUNTRY, FKGD_HAS_COUNTRY
    FROM CUST_ADDRESS
    WHERE ADDRESS_TYPE = '4' AND ENTRY_STATUS = '1'
    AND (FK_CUSTOMERCUST_ID, SERIAL_NUM) IN (
        SELECT FK_CUSTOMERCUST_ID, MIN(SERIAL_NUM)
        FROM CUST_ADDRESS
        WHERE ADDRESS_TYPE = '4' AND ENTRY_STATUS = '1'
        GROUP BY FK_CUSTOMERCUST_ID
    )
) addr_work ON addr_work.FK_CUSTOMERCUST_ID = c.CUST_ID
LEFT JOIN GENERIC_DETAIL work_country ON work_country.FK_GENERIC_HEADPAR = addr_work.FKGH_HAS_COUNTRY
                                     AND work_country.SERIAL_NUM = addr_work.FKGD_HAS_COUNTRY

WHERE c.ENTRY_STATUS = '1'  -- Active customers only
ORDER BY c.CUST_ID;

-- Performance Notes:
-- 1. This query uses subqueries for categories to improve performance
-- 2. Date validation ensures only valid dates are returned
-- 3. TRIM functions clean up data with extra spaces
-- 4. COALESCE provides default values for missing data
-- 5. Filters ensure only active customers and addresses are included