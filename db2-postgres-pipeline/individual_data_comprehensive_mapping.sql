-- Comprehensive Individual Information Mapping
-- Direct mapping from source tables based on CUST_DETAILS_FULL_VW analysis

SELECT 
    -- REPORTING INFORMATION
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
    CAST(c.CUST_ID AS VARCHAR(50)) AS customerIdentificationNumber,
    
    -- PERSONAL PARTICULARS
    c.FIRST_NAME AS firstName,
    c.MIDDLE_NAME AS middleNames,
    CAST(NULL AS VARCHAR(100)) AS otherNames,  -- Not available
    TRIM(
        COALESCE(c.FIRST_NAME, '') || ' ' || 
        COALESCE(c.MIDDLE_NAME, '') || ' ' || 
        COALESCE(c.SURNAME, '')
    ) AS fullNames,
    c.SURNAME AS presentSurname,
    c.MOTHER_SURNAME AS birthSurname,
    CASE 
        WHEN c.SEX = 'M' THEN 'Male'
        WHEN c.SEX = 'F' THEN 'Female'
        ELSE 'NotSpecified'
    END AS gender,
    family_cat.DESCRIPTION AS maritalStatus,
    CAST(NULL AS INTEGER) AS numberSpouse,  -- Not available
    national_cat.DESCRIPTION AS nationality,
    citizen_cat.DESCRIPTION AS citizenship,
    CASE 
        WHEN c.NON_RESIDENT = '0' THEN 'Resident'
        WHEN c.NON_RESIDENT = '1' THEN 'Non-Resident'
        ELSE 'Unknown'
    END AS residency,
    profes_cat.DESCRIPTION AS profession,
    indcode_cat.DESCRIPTION AS sectorSnaClassification,
    CASE 
        WHEN c.ENTRY_STATUS = '0' THEN 'Deceased'
        ELSE 'Active'
    END AS fateStatus,
    proflevl_cat.DESCRIPTION AS socialStatus,
    profcat_cat.DESCRIPTION AS employmentStatus,
    c.SALARY_AMN AS monthlyIncome,
    c.NUM_OF_CHILDREN AS numberDependants,
    edulevel_cat.DESCRIPTION AS educationLevel,
    CAST(NULL AS DECIMAL(15,2)) AS averageMonthlyExpenditure,  -- Not available
    CASE 
        WHEN c.BLACKLISTED_IND = '1' THEN 'Blacklisted'
        WHEN c.AML_STATUS IS NOT NULL THEN c.AML_STATUS
        ELSE 'Clean'
    END AS negativeClientStatus,
    
    -- SPOUSE INFORMATION
    c.SPOUSE_NAME AS spousesFullName,
    CAST(NULL AS VARCHAR(50)) AS spouseIdentificationType,  -- Not available
    CAST(NULL AS VARCHAR(50)) AS spouseIdentificationNumber,  -- Not available
    c.MOTHER_SURNAME AS maidenName,
    CAST(NULL AS DECIMAL(15,2)) AS monthlyExpenses,  -- Not available
    
    -- BIRTH INFORMATION
    VARCHAR_FORMAT(c.DATE_OF_BIRTH, 'DDMMYYYYHHMM') AS birthDate,
    bcountry_cat.DESCRIPTION AS birthCountry,
    CAST(NULL AS VARCHAR(20)) AS birthPostalCode,  -- Not available
    CAST(NULL AS VARCHAR(20)) AS birthHouseNumber,  -- Not available
    region_cat.DESCRIPTION AS birthRegion,
    CAST(NULL AS VARCHAR(100)) AS birthDistrict,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS birthWard,  -- Not available
    c.BIRTHPLACE AS birthStreet,
    
    -- IDENTIFICATION INFORMATION
    id_type.DESCRIPTION AS identificationType,
    oid.ID_NO AS identificationNumber,
    VARCHAR_FORMAT(oid.ISSUE_DATE, 'DDMMYYYYHHMM') AS issuanceDate,
    VARCHAR_FORMAT(oid.EXPIRY_DATE, 'DDMMYYYYHHMM') AS expirationDate,
    id_country.DESCRIPTION AS issuancePlace,
    CAST(NULL AS VARCHAR(200)) AS issuingAuthority,  -- Not available
    
    -- BUSINESS INFORMATION
    CASE 
        WHEN c.CUST_TYPE = '2' THEN c.SURNAME  -- Company name for corporate
        ELSE NULL
    END AS businessName,
    VARCHAR_FORMAT(c.CUSTOMER_BEGIN_DAT, 'DDMMYYYYHHMM') AS establishmentDate,
    c.CHAMBER_ID AS businessRegistrationNumber,
    VARCHAR_FORMAT(c.CERTIFIC_DATE, 'DDMMYYYYHHMM') AS businessRegistrationDate,
    CAST(NULL AS VARCHAR(50)) AS businessLicenseNumber,  -- Not available
    afm.AFM_NO AS taxIdentificationNumber,
    
    -- EMPLOYER INFORMATION
    c.EMPLOYER AS employerName,
    CAST(NULL AS VARCHAR(100)) AS employerRegion,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS employerDistrict,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS employerWard,  -- Not available
    c.EMPLOYER_ADDRESS AS employerStreet,
    CAST(NULL AS VARCHAR(20)) AS employerHouseNumber,  -- Not available
    CAST(NULL AS VARCHAR(20)) AS employerPostalCode,  -- Not available
    activity_cat.DESCRIPTION AS businessNature,
    
    -- INDIVIDUAL CONTACTS
    c.MOBILE_TEL AS mobileNumber,
    c.MOBILE_TEL2 AS alternativeMobileNumber,
    c.TELEPHONE_1 AS fixedLineNumber,
    addr_comm.FAX_NO AS faxNumber,
    c.E_MAIL AS emailAddress,
    c.INTERNET_ADDRESS AS socialMedia,
    addr_comm.ADDRESS_1 AS mainAddress,
    
    -- PRIMARY ADDRESS (Communication Address)
    addr_comm.ADDRESS_1 AS street,
    CAST(NULL AS VARCHAR(20)) AS houseNumber,  -- Not available
    addr_comm.ZIP_CODE AS postalCode,
    addr_comm.REGION AS region,
    CAST(NULL AS VARCHAR(100)) AS district,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS ward,  -- Not available
    comm_country.DESCRIPTION AS country,
    
    -- SECONDARY ADDRESS (Work Address)
    addr_work.ADDRESS_1 AS secondaryStreet,
    CAST(NULL AS VARCHAR(20)) AS secondaryHouseNumber,  -- Not available
    addr_work.ZIP_CODE AS secondaryPostalCode,
    addr_work.REGION AS secondaryRegion,
    CAST(NULL AS VARCHAR(100)) AS secondaryDistrict,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS secondaryWard,  -- Not available
    work_country.DESCRIPTION AS secondaryCountry

FROM CUSTOMER c

-- Customer Categories with proper joins
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

-- Identification Information
LEFT JOIN OTHER_ID oid ON oid.FK_CUSTOMERCUST_ID = c.CUST_ID 
                      AND COALESCE(oid.MAIN_FLAG, '1') = '1'
LEFT JOIN GENERIC_DETAIL id_type ON id_type.FK_GENERIC_HEADPAR = oid.FKGH_HAS_TYPE
                                AND id_type.SERIAL_NUM = oid.FKGD_HAS_TYPE
LEFT JOIN GENERIC_DETAIL id_country ON id_country.FK_GENERIC_HEADPAR = oid.FKGH_HAS_BEEN_ISSU
                                   AND id_country.SERIAL_NUM = oid.FKGD_HAS_BEEN_ISSU

-- Tax Information
LEFT JOIN OTHER_AFM afm ON afm.FK_CUSTOMERCUST_ID = c.CUST_ID 
                       AND COALESCE(afm.MAIN_FLAG, '1') = '1'

-- Address Information
LEFT JOIN CUST_ADDRESS addr_comm ON addr_comm.FK_CUSTOMERCUST_ID = c.CUST_ID
                                AND addr_comm.COMMUNICATION_ADDR = '1'
                                AND addr_comm.ENTRY_STATUS = '1'
LEFT JOIN GENERIC_DETAIL comm_country ON comm_country.FK_GENERIC_HEADPAR = addr_comm.FKGH_HAS_COUNTRY
                                     AND comm_country.SERIAL_NUM = addr_comm.FKGD_HAS_COUNTRY

LEFT JOIN CUST_ADDRESS addr_work ON addr_work.FK_CUSTOMERCUST_ID = c.CUST_ID
                                AND addr_work.ADDRESS_TYPE = '4'
                                AND addr_work.ENTRY_STATUS = '1'
LEFT JOIN GENERIC_DETAIL work_country ON work_country.FK_GENERIC_HEADPAR = addr_work.FKGH_HAS_COUNTRY
                                     AND work_country.SERIAL_NUM = addr_work.FKGD_HAS_COUNTRY

WHERE c.ENTRY_STATUS = '1'  -- Active customers only
ORDER BY c.CUST_ID;