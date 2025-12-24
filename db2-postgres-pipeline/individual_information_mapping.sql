-- Individual Information Data Mapping based on CUST_DETAILS_FULL_VW
-- This query maps the customer view fields to the Individual Information API requirements

SELECT 
    -- REPORTING INFORMATION
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
    CAST(cv.CUST_ID AS VARCHAR(50)) AS customerIdentificationNumber,
    
    -- PERSONAL PARTICULARS
    cv.FIRST_NAME AS firstName,
    cv.MIDDLE_NAME AS middleNames,
    CAST(NULL AS VARCHAR(100)) AS otherNames,  -- Not available in current schema
    TRIM(
        COALESCE(cv.FIRST_NAME, '') || ' ' || 
        COALESCE(cv.MIDDLE_NAME, '') || ' ' || 
        COALESCE(cv.SURNAME, '')
    ) AS fullNames,
    cv.SURNAME AS presentSurname,
    cv.MOTHER_SURNAME AS birthSurname,
    CASE 
        WHEN cv.SEX = 'M' THEN 'Male'
        WHEN cv.SEX = 'F' THEN 'Female'
        ELSE 'NotSpecified'
    END AS gender,
    cv.FAMILY AS maritalStatus,
    CAST(NULL AS INTEGER) AS numberSpouse,  -- Not directly available
    cv.NATIONAL_DESCRIPTION AS nationality,
    cv.CITIZEN_DESCRIPTION AS citizenship,
    CASE 
        WHEN cv.NON_RESIDENT = '0' THEN 'Resident'
        WHEN cv.NON_RESIDENT = '1' THEN 'Non-Resident'
        ELSE 'Unknown'
    END AS residency,
    cv.PROFES AS profession,
    cv.INDCODE AS sectorSnaClassification,
    CASE 
        WHEN cv.ENTRY_STATUS = '0' THEN 'Deceased'
        ELSE 'Active'
    END AS fateStatus,
    cv.PROFLEVL AS socialStatus,
    cv.PROFCAT AS employmentStatus,
    cv.SALARY_AMN AS monthlyIncome,
    cv.NUM_OF_CHILDREN AS numberDependants,
    cv.EDULEVEL AS educationLevel,
    CAST(NULL AS DECIMAL(15,2)) AS averageMonthlyExpenditure,  -- Not available
    CASE 
        WHEN cv.BLACKLISTED_IND = '1' THEN 'Blacklisted'
        WHEN cv.AML_STATUS IS NOT NULL THEN cv.AML_STATUS
        ELSE 'Clean'
    END AS negativeClientStatus,
    
    -- SPOUSE INFORMATION
    cv.SPOUSE_NAME AS spousesFullName,
    CAST(NULL AS VARCHAR(50)) AS spouseIdentificationType,  -- Not available
    CAST(NULL AS VARCHAR(50)) AS spouseIdentificationNumber,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS maidenName,  -- Could use MOTHER_SURNAME
    CAST(NULL AS DECIMAL(15,2)) AS monthlyExpenses,  -- Not available
    
    -- BIRTH INFORMATION
    VARCHAR_FORMAT(cv.DATE_OF_BIRTH, 'DDMMYYYYHHMM') AS birthDate,
    cv.B_COUNTRY AS birthCountry,
    CAST(NULL AS VARCHAR(20)) AS birthPostalCode,  -- Not available
    CAST(NULL AS VARCHAR(20)) AS birthHouseNumber,  -- Not available
    cv.REGION AS birthRegion,
    CAST(NULL AS VARCHAR(100)) AS birthDistrict,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS birthWard,  -- Not available
    cv.BIRTHPLACE AS birthStreet,
    
    -- IDENTIFICATION INFORMATION
    cv.OTHER_ID_DESC AS identificationType,
    cv.ID_NO AS identificationNumber,
    VARCHAR_FORMAT(cv.ID_ISSUE_DATE, 'DDMMYYYYHHMM') AS issuanceDate,
    VARCHAR_FORMAT(cv.ID_EXPIRY_DATE, 'DDMMYYYYHHMM') AS expirationDate,
    cv.ID_COUNTRY_DESCRIPTION AS issuancePlace,
    CAST(NULL AS VARCHAR(200)) AS issuingAuthority,  -- Not available
    
    -- BUSINESS INFORMATION (for corporate customers)
    CASE 
        WHEN cv.CUST_TYPE = '2' THEN cv.SURNAME  -- Company name for corporate
        ELSE NULL
    END AS businessName,
    VARCHAR_FORMAT(cv.CUSTOMER_BEGIN_DAT, 'DDMMYYYYHHMM') AS establishmentDate,
    cv.CHAMBER_ID AS businessRegistrationNumber,
    VARCHAR_FORMAT(cv.CERTIFIC_DATE, 'DDMMYYYYHHMM') AS businessRegistrationDate,
    CAST(NULL AS VARCHAR(50)) AS businessLicenseNumber,  -- Not available
    cv.AFM_NO AS taxIdentificationNumber,
    
    -- EMPLOYER INFORMATION
    cv.EMPLOYER AS employerName,
    CAST(NULL AS VARCHAR(100)) AS employerRegion,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS employerDistrict,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS employerWard,  -- Not available
    cv.EMPLOYER_ADDRESS AS employerStreet,
    CAST(NULL AS VARCHAR(20)) AS employerHouseNumber,  -- Not available
    CAST(NULL AS VARCHAR(20)) AS employerPostalCode,  -- Not available
    cv.ACTIVITY AS businessNature,
    
    -- INDIVIDUAL CONTACTS
    cv.MOBILE_TEL AS mobileNumber,
    cv.MOBILE_TEL2 AS alternativeMobileNumber,
    cv.TELEPHONE_1 AS fixedLineNumber,
    cv.FAX_NO AS faxNumber,
    cv.E_MAIL AS emailAddress,
    cv.INTERNET_ADDRESS AS socialMedia,
    cv.C_ADDRESS_1 AS mainAddress,
    
    -- PRIMARY ADDRESS
    cv.C_ADDRESS_1 AS street,
    CAST(NULL AS VARCHAR(20)) AS houseNumber,  -- Not available
    cv.C_ZIP_CODE AS postalCode,
    cv.C_REGION AS region,
    CAST(NULL AS VARCHAR(100)) AS district,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS ward,  -- Not available
    cv.C_COUNTRY AS country,
    
    -- SECONDARY ADDRESS (Work Address)
    cv.W_ADDRESS_1 AS secondaryStreet,
    CAST(NULL AS VARCHAR(20)) AS secondaryHouseNumber,  -- Not available
    cv.W_ZIP_CODE AS secondaryPostalCode,
    cv.W_REGION AS secondaryRegion,
    CAST(NULL AS VARCHAR(100)) AS secondaryDistrict,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS secondaryWard,  -- Not available
    cv.W_COUNTRY AS secondaryCountry,
    
    -- ADDITIONAL FIELDS FOR REFERENCE
    cv.CUST_TYPE AS customerType,
    cv.ENTRY_STATUS AS entryStatus,
    cv.LAST_UPDATE AS lastUpdate,
    cv.CUSTOMER_BEGIN_DAT AS customerBeginDate

FROM CUST_DETAILS_FULL_VW cv
WHERE cv.ENTRY_STATUS = '1'  -- Active customers only
ORDER BY cv.CUST_ID;