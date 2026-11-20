WITH district_wards AS (
    SELECT DISTINCT DISTRICT,
                    WARD,
                    ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY WARD) AS rn,
                    COUNT(*) OVER (PARTITION BY DISTRICT)                   AS total_wards
    FROM bank_location_lookup_v2
),
region_districts AS (
    SELECT REGION,
           DISTRICT,
           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT) AS rn,
           COUNT(*) OVER (PARTITION BY REGION)                       AS total_districts
    FROM bank_location_lookup_v2
    GROUP BY REGION, DISTRICT
),
location_regions AS (
    SELECT REGION,
           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REGION) AS rn
    FROM bank_location_lookup_v2
    GROUP BY REGION
),
location_districts AS (
    SELECT REGION,
           DISTRICT,
           ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY REGION) AS rn
    FROM bank_location_lookup_v2
    GROUP BY REGION, DISTRICT
),
location_wards AS (
    SELECT REGION,
           DISTRICT,
           WARD,
           ROW_NUMBER() OVER (PARTITION BY WARD ORDER BY REGION) AS rn
    FROM bank_location_lookup_v2
    GROUP BY REGION, DISTRICT, WARD
)

SELECT CURRENT_TIMESTAMP                                                                                AS reportingDate,
       TRIM(c.cust_id)                                                                                  AS customerIdentificationNumber,
       COALESCE(NULLIF(TRIM(c.first_name), ''), 'N/A')                                                  AS firstName,
       COALESCE(NULLIF(TRIM(c.middle_name), ''), 'N/A')                                                 AS middleNames,
       COALESCE(NULLIF(TRIM(c.surname), ''), 'N/A')                                                     AS otherNames,
       TRIM(
               CASE
                   WHEN c.cust_type = '1' THEN
                       TRIM(NVL(c.first_name, '')) || ' ' ||
                       TRIM(NVL(c.middle_name, '')) || ' ' ||
                       TRIM(NVL(c.surname, ''))
                   WHEN c.cust_type = '2' THEN TRIM(c.surname)
                   ELSE ''
                   END
       )                                                                                                AS fullNames,
       c.surname                                                                                        AS presentSurname,
       c.surname                                                                                        AS birthSurname,
       CASE
           WHEN c.sex = 'M' THEN 'Male'
           WHEN c.sex = 'F' THEN 'Female'
           ELSE 'Not Applicable'
           END                                                                                          AS gender,
       CASE UPPER(TRIM(gd_family.description))
           WHEN 'MARRIED'  THEN 'Married'
           WHEN 'SINGLE'   THEN 'Single'
           WHEN 'DIVORCED' THEN 'Divorced'
           WHEN 'WIDOWED'  THEN 'Widowed'
           ELSE 'Single'
           END                                                                                          AS maritalStatus,
       NULL                                                                                             AS numberSpouse,
       CASE UPPER(TRIM(gd_natio.description))
           WHEN 'TANZANIAN' THEN 'TANZANIA, UNITED REPUBLIC OF'
           END                                                                                          AS nationality,
       CASE UPPER(TRIM(gd_natio.description))
           WHEN 'TANZANIAN' THEN 'TANZANIA, UNITED REPUBLIC OF'
           END                                                                                          AS citizenship,
       CASE
           WHEN c.non_resident = '0' THEN 'Resident'
           ELSE 'Non-Resident'
           END                                                                                          AS residency,
       gd_proff.description                                                                             AS profession,
       'Households'                                                                                     AS sectorSnaClassification,
       CASE c.CUST_STATUS
           WHEN '5' THEN 'Disappeared'
           ELSE 'No Fate'
           END                                                                                          AS fateStatus,
       'N/A'                                                                                            AS socialStatus,
       CASE UPPER(TRIM(gd_employment.description))
           WHEN 'EMPLOYED'         THEN 'Employed'
           WHEN 'SALARIED'         THEN 'Employed'
           WHEN 'CUSTOMER SERVICE' THEN 'Self-employed'
           ELSE 'Unemployed'
           END                                                                                          AS employmentStatus,
       gd_customer_income.DESCRIPTION                                                                   AS monthlyIncome,
       (c.num_of_children + c.children_above18)                                                         AS numberDependants,
       COALESCE(gd_edulevel.description, 'Basic')                                                       AS educationLevel,
       0.00                                                                                             AS averageMonthlyExpenditure,
       c.blacklisted_ind                                                                                AS negativeClientStatus,
       c.spouse_name                                                                                    AS spousesFullName,
       NULL                                                                                             AS spouseIdentificationType,
       NULL                                                                                             AS spouseIdentificationNumber,
       NULL                                                                                             AS maidenName,
       NULL                                                                                             AS monthlyExpenses,
       c.date_of_birth                                                                                  AS birthDate,
       CASE UPPER(TRIM(id_country.description))
           WHEN 'TANZANIA' THEN 'TANZANIA, UNITED REPUBLIC OF'
           END                                                                                          AS birthCountry,
       NULL                                                                                             AS birthPostalCode,
       NULL                                                                                             AS birthHouseNumber,
       COALESCE(
               loc_region_birth_city.REGION,
               loc_birth_region_from_district.REGION,
               loc_birth_region_from_ward.REGION,
               loc_region_city.REGION,
               loc_region_dist.REGION,
               loc_region_from_district.REGION,
               loc_region_from_ward.REGION,
               'Dar es Salaam'
       )                                                                                                AS birthRegion,
       birth_district_pick.DISTRICT                                                                     AS birthDistrict,
       NULL                                                                                             AS birthWard,
       NULL                                                                                             AS birthStreet,

       -- identificationType: full BOT mapping including OIDTP/6 prefix logic
       CASE UPPER(TRIM(idt.description))
           WHEN 'NATIONAL IDENTITY CARD'                   THEN 'NationalIdentityCard'
           WHEN 'DRIVING LICENCE'                          THEN 'DrivingLicense'
           WHEN 'VOTERS ID'                                THEN 'VotersRegistrationCard'
           WHEN 'PASSPORT'                                 THEN 'Passport'
           WHEN 'REFUGEE ID CARD'                          THEN 'Foreign Unique ID'
           WHEN 'MIGRATION'                                THEN 'Foreign Unique ID'
           WHEN 'ARMED FORCES ID'                          THEN 'Employee ID'
           WHEN 'POLICEMENS ID'                            THEN 'Employee ID'
           WHEN 'STUDENT ID'                               THEN 'Student ID'
           WHEN 'COMPANYS REGISTRY NUMBER'                 THEN 'Certificate of Registration'
           WHEN 'REG. CERTIFICATE(NON-LIMITED COMPANIES)'  THEN 'Certificate of Registration'
           WHEN 'CERTIFICATE OF INCORPORATION(LIMITED CO)' THEN 'Certificate of Incorporation'
           WHEN 'OTHER TYPE OF IDENTIFICATION'             THEN
               CASE
                   WHEN SUBSTR(TRIM(id.id_no), 1, 3) IN (
                       'KMC','KDC','MDC','BDC','MMC','SMC','KTC','IMC','RDC',
                       'BMC','UDC','LDC','IDC','MCC','GDC','WDC','NDC','NTC',
                       'SDC','HDC','MKD','CDC','ADC','HMD','KIN','MAS','ARU',
                       'DOD','MBY','DSM','MOR','KIL','KST'
                   ) THEN 'Employee ID'
                   WHEN SUBSTR(TRIM(id.id_no), 1, 2) = 'PF'
                     OR SUBSTR(TRIM(id.id_no), 1, 3) IN (
                       'PF0','PF1','PF2','PF3','PF4','PF5',
                       'PF6','PF7','PF8','PF9','PFN','PFS','PF/'
                     ) THEN 'Pension Fund ID'
                   WHEN SUBSTR(TRIM(id.id_no), 1, 2) IN ('ED','EL')
                     THEN 'Employee ID'
                   ELSE 'Employee ID'
               END
           ELSE 'NationalIdentityCard'
       END                                                                                              AS identificationType,

       id.id_no                                                                                         AS identificationNumber,
       CASE
           WHEN id.issue_date = DATE '0001-01-01' THEN null
           ELSE TO_CHAR(id.issue_date, 'YYYY-MM-DD')
           END                                                                                          AS issuance_date,
       CASE
           WHEN id.expiry_date = DATE '0001-01-01' THEN null
           ELSE TO_CHAR(id.expiry_date, 'YYYY-MM-DD')
           END                                                                                          AS expirationDate,
       'Dar es Salaam'                                                                                  AS issuancePlace,

       -- issuingAuthority: full mapping including OIDTP/6 prefix logic
       CASE UPPER(TRIM(idt.description))
           WHEN 'NATIONAL IDENTITY CARD'                   THEN 'National Identification Authority (NIDA)'
           WHEN 'DRIVING LICENCE'                          THEN 'Tanzania Revenue Authority (TRA)'
           WHEN 'VOTERS ID'                                THEN 'Independent National Electoral Commission (INEC)'
           WHEN 'PASSPORT'                                 THEN 'Immigration Services Department'
           WHEN 'REFUGEE ID CARD'                          THEN 'Tanzania Immigration Services Department'
           WHEN 'MIGRATION'                                THEN 'Tanzania Immigration Services Department'
           WHEN 'ARMED FORCES ID'                          THEN 'Tanzania People Defence Force (TPDF)'
           WHEN 'POLICEMENS ID'                            THEN 'Tanzania Police Force'
           WHEN 'STUDENT ID'                               THEN 'Recognized Education Institution'
           WHEN 'COMPANYS REGISTRY NUMBER'                 THEN 'Business Registrations and Licensing Agency (BRELA)'
           WHEN 'REG. CERTIFICATE(NON-LIMITED COMPANIES)'  THEN 'Business Registrations and Licensing Agency (BRELA)'
           WHEN 'CERTIFICATE OF INCORPORATION(LIMITED CO)' THEN 'Business Registrations and Licensing Agency (BRELA)'
           WHEN 'OTHER TYPE OF IDENTIFICATION'             THEN
               CASE
                   WHEN SUBSTR(TRIM(id.id_no), 1, 3) IN (
                       'KMC','KDC','MDC','BDC','MMC','SMC','KTC','IMC','RDC',
                       'BMC','UDC','LDC','IDC','MCC','GDC','WDC','NDC','NTC',
                       'SDC','HDC','MKD','CDC','ADC','HMD','KIN','MAS','ARU',
                       'DOD','MBY','DSM','MOR','KIL','KST'
                   ) THEN 'Respective Employer'
                   WHEN SUBSTR(TRIM(id.id_no), 1, 2) = 'PF'
                     OR SUBSTR(TRIM(id.id_no), 1, 3) IN (
                       'PF0','PF1','PF2','PF3','PF4','PF5',
                       'PF6','PF7','PF8','PF9','PFN','PFS','PF/'
                     ) THEN 'Respective Pension Fund'
                   WHEN SUBSTR(TRIM(id.id_no), 1, 2) IN ('ED','EL')
                     THEN 'Respective Employer'
                   ELSE 'Respective Employer'
               END
           ELSE NULL
       END                                                                                              AS issuingAuthority,

       NULL                                                                                             AS businessName,
       NULL                                                                                             AS establishmentDate,
       NULL                                                                                             AS businessRegistrationNumber,
       NULL                                                                                             AS businessRegistrationDate,
       NULL                                                                                             AS businessLicenseNumber,
       NULL                                                                                             AS taxIdentificationNumber,
       NULL                                                                                             AS employerName,
       NULL                                                                                             AS employerRegion,
       NULL                                                                                             AS employerDistrict,
       NULL                                                                                             AS employerWard,
       NULL                                                                                             AS employerStreet,
       NULL                                                                                             AS employerHouseNumber,
       NULL                                                                                             AS employerPostalCode,
       NULL                                                                                             AS businessNature,
       c.mobile_tel                                                                                     AS mobileNumber,
       c.mobile_tel2                                                                                    AS alternativeMobileNumber,
       c.telephone_1                                                                                    AS fixedLineNumber,
       c_address.fax_no                                                                                 AS faxNumber,
       c.e_mail                                                                                         AS emailAddress,
       c.internet_address                                                                               AS socialMedia,
       ward_selection.WARD                                                                              AS mainAddress,
       NULL                                                                                             AS street,
       NULL                                                                                             AS houseNumber,
       ward_selection.WARD                                                                              AS postalCode,
       COALESCE(
               loc_region_city.REGION,
               loc_region_dist.REGION,
               loc_region_from_district.REGION,
               loc_region_from_ward.REGION,
               'Dar es Salaam'
       )                                                                                                AS region,
       COALESCE(
               loc_district_region.DISTRICT,
               loc_district_from_ward.DISTRICT,
               loc_district_from_city.DISTRICT,
               loc_district_from_region.DISTRICT
       )                                                                                                AS district,
       ward_selection.WARD                                                                              AS ward,
       c_country.description                                                                            AS country,
       NULL                                                                                             AS sstreet,
       NULL                                                                                             AS shouseNumber,
       NULL                                                                                             AS spostalCode,
       NULL                                                                                             AS sregion,
       NULL                                                                                             AS sdistrict,
       NULL                                                                                             AS sward,
       NULL                                                                                             AS scountry

FROM customer c

         LEFT JOIN cust_address c_address
                   ON c_address.fk_customercust_id = c.cust_id
                       AND c_address.communication_addr = '1'
                       AND c_address.entry_status = '1'

         LEFT JOIN location_regions loc_region_birth_city
                   ON loc_region_birth_city.rn = 1
                       AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(C.BIRTHPLACE)), ' ', ''), '-', ''), '_', ''), ',', '')
                          LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_region_birth_city.REGION)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'

         LEFT JOIN location_districts loc_birth_region_from_district
                   ON loc_region_birth_city.REGION IS NULL
                       AND loc_birth_region_from_district.rn = 1
                       AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c.BIRTHPLACE)), ' ', ''), '-', ''), '_', ''), ',', '')
                          LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_birth_region_from_district.DISTRICT)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'

         LEFT JOIN location_wards loc_birth_region_from_ward
                   ON loc_region_birth_city.REGION IS NULL
                       AND loc_birth_region_from_district.REGION IS NULL
                       AND loc_birth_region_from_ward.rn = 1
                       AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c.BIRTHPLACE)), ' ', ''), '-', ''), '_', ''), ',', '')
                          LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_birth_region_from_ward.WARD)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'

         LEFT JOIN location_regions loc_region_city
                   ON loc_region_city.rn = 1
                       AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.CITY)), ' ', ''), '-', ''), '_', ''), ',', '')
                          LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_region_city.REGION)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'

         LEFT JOIN location_regions loc_region_dist
                   ON loc_region_city.REGION IS NULL
                       AND loc_region_dist.rn = 1
                       AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''), '-', ''), '_', ''), ',', '')
                          LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_region_dist.REGION)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'

         LEFT JOIN location_districts loc_region_from_district
                   ON loc_region_city.REGION IS NULL
                       AND loc_region_dist.REGION IS NULL
                       AND loc_region_from_district.rn = 1
                       AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''), '-', ''), '_', ''), ',', '')
                          LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_region_from_district.DISTRICT)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'

         LEFT JOIN location_wards loc_region_from_ward
                   ON loc_region_city.REGION IS NULL
                       AND loc_region_dist.REGION IS NULL
                       AND loc_region_from_district.REGION IS NULL
                       AND loc_region_from_ward.rn = 1
                       AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.ADDRESS_1)), ' ', ''), '-', ''), '_', ''), ',', '')
                          LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_region_from_ward.WARD)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'

         LEFT JOIN location_districts loc_district_region
                   ON loc_district_region.rn = 1
                       AND COALESCE(
                               loc_region_city.REGION,
                               loc_region_dist.REGION,
                               loc_region_from_district.REGION,
                               loc_region_from_ward.REGION
                           ) = loc_district_region.REGION
                       AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''), '-', ''), '_', ''), ',', '')
                          LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_district_region.DISTRICT)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'

         LEFT JOIN location_wards loc_district_from_ward
                   ON loc_district_region.DISTRICT IS NULL
                       AND loc_district_from_ward.rn = 1
                       AND COALESCE(
                               loc_region_city.REGION,
                               loc_region_dist.REGION,
                               loc_region_from_district.REGION,
                               loc_region_from_ward.REGION
                           ) = loc_district_from_ward.REGION
                       AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.ADDRESS_1)), ' ', ''), '-', ''), '_', ''), ',', '')
                          LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_district_from_ward.WARD)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'

         LEFT JOIN location_districts loc_district_from_city
                   ON loc_district_region.DISTRICT IS NULL
                       AND loc_district_from_ward.DISTRICT IS NULL
                       AND loc_district_from_city.rn = 1
                       AND COALESCE(
                               loc_region_city.REGION,
                               loc_region_dist.REGION,
                               loc_region_from_district.REGION,
                               loc_region_from_ward.REGION
                           ) = loc_district_from_city.REGION
                       AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.CITY)), ' ', ''), '-', ''), '_', ''), ',', '')
                          LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_district_from_city.DISTRICT)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'

         LEFT JOIN location_districts loc_district_from_region
                   ON loc_district_region.DISTRICT IS NULL
                       AND loc_district_from_ward.DISTRICT IS NULL
                       AND loc_district_from_city.DISTRICT IS NULL
                       AND loc_district_from_region.rn = 1
                       AND loc_district_from_region.REGION = COALESCE(
                               loc_region_city.REGION,
                               loc_region_dist.REGION,
                               loc_region_from_district.REGION,
                               loc_region_from_ward.REGION,
                               'Dar es Salaam'
                           )

         LEFT JOIN district_wards ward_selection
                   ON ward_selection.DISTRICT = COALESCE(
                           loc_district_region.DISTRICT,
                           loc_district_from_ward.DISTRICT,
                           loc_district_from_city.DISTRICT,
                           loc_district_from_region.DISTRICT,
                           'Dar es Salaam'
                       )
                       AND ward_selection.rn = MOD(ASCII(SUBSTR(TRIM(c.CUST_ID), 1, 1)), ward_selection.total_wards) + 1

         LEFT JOIN region_districts birth_district_pick
                   ON birth_district_pick.REGION = COALESCE(
                           loc_region_birth_city.REGION,
                           loc_birth_region_from_district.REGION,
                           loc_birth_region_from_ward.REGION,
                           loc_region_city.REGION,
                           loc_region_dist.REGION,
                           loc_region_from_district.REGION,
                           loc_region_from_ward.REGION,
                           'Dar es Salaam'
                       )
                       AND birth_district_pick.rn = MOD(
                               ASCII(SUBSTR(TRIM(c.CUST_ID), 1, 1)),
                               birth_district_pick.total_districts
                           ) + 1

         LEFT JOIN generic_detail c_country
                   ON c_address.fkgd_has_country = c_country.serial_num
                       AND c_address.fkgh_has_country = c_country.fk_generic_headpar

         LEFT JOIN other_id id
                   ON id.fk_customercust_id = c.cust_id
                       AND (CASE WHEN id.serial_no IS NULL THEN '1' ELSE id.main_flag END) = '1'

         LEFT JOIN generic_detail id_country
                   ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                       AND id.fkgd_has_been_issu = id_country.serial_num

         LEFT JOIN generic_detail idt
                   ON idt.fk_generic_headpar = id.fkgh_has_type
                       AND idt.serial_num = id.fkgd_has_type

         LEFT JOIN customer_category cc_family
                   ON cc_family.fk_customercust_id = c.cust_id
                       AND cc_family.fk_categorycategor = 'FAMILY'
                       AND cc_family.fk_generic_detafk = 'FALST'
         LEFT JOIN generic_detail gd_family
                   ON gd_family.fk_generic_headpar = cc_family.fk_generic_detafk
                       AND gd_family.serial_num = cc_family.fk_generic_detaser

         LEFT JOIN customer_category cc_employment
                   ON cc_employment.fk_customercust_id = c.cust_id
                       AND cc_employment.fk_categorycategor = 'PROFLEVL'
                       AND cc_employment.fk_generic_detafk = 'PRFST'
         LEFT JOIN generic_detail gd_employment
                   ON gd_employment.fk_generic_headpar = cc_employment.fk_generic_detafk
                       AND gd_employment.serial_num = cc_employment.fk_generic_detaser

         LEFT JOIN customer_category cc_natio
                   ON cc_natio.fk_customercust_id = c.cust_id
                       AND cc_natio.fk_categorycategor = 'NATIONAL'
                       AND cc_natio.fk_generic_detafk = 'NATIO'
         LEFT JOIN generic_detail gd_natio
                   ON gd_natio.fk_generic_headpar = cc_natio.fk_generic_detafk
                       AND gd_natio.serial_num = cc_natio.fk_generic_detaser

         LEFT JOIN customer_category cc_citiz
                   ON cc_citiz.fk_customercust_id = c.cust_id
                       AND cc_citiz.fk_categorycategor = 'CITIZEN'
                       AND cc_citiz.fk_generic_detafk = 'CITIZ'
         LEFT JOIN generic_detail gd_citiz
                   ON gd_citiz.fk_generic_headpar = cc_citiz.fk_generic_detafk
                       AND gd_citiz.serial_num = cc_citiz.fk_generic_detaser

         LEFT JOIN customer_category cc_proff
                   ON cc_proff.fk_customercust_id = c.cust_id
                       AND cc_proff.fk_categorycategor = 'PROFES'
                       AND cc_proff.fk_generic_detafk = 'PROFF'
         LEFT JOIN generic_detail gd_proff
                   ON gd_proff.fk_generic_headpar = cc_proff.fk_generic_detafk
                       AND gd_proff.serial_num = cc_proff.fk_generic_detaser

         LEFT JOIN customer_category cc_edulevel
                   ON cc_edulevel.fk_customercust_id = c.cust_id
                       AND cc_edulevel.fk_categorycategor = 'EDULEVEL'
                       AND cc_edulevel.fk_generic_detafk = 'EDULV'
         LEFT JOIN generic_detail gd_edulevel
                   ON gd_edulevel.fk_generic_headpar = cc_edulevel.fk_generic_detafk
                       AND gd_edulevel.serial_num = cc_edulevel.fk_generic_detaser

         LEFT JOIN customer_category customer_income
                   ON customer_income.fk_customercust_id = c.cust_id
                       AND customer_income.fk_categorycategor = 'INCLEVEL'
                       AND customer_income.fk_generic_detafk = 'INCLV'
         LEFT JOIN generic_detail gd_customer_income
                   ON gd_customer_income.fk_generic_headpar = customer_income.fk_generic_detafk
                       AND gd_customer_income.serial_num = customer_income.fk_generic_detaser

WHERE c.CUST_TYPE = '1'

  -- Birth certificate has no BOT equivalent — excluded
  AND UPPER(TRIM(idt.description)) NOT IN ('BIRTH CERTIFICATE')

  -- NIN validation: exclude genuine short NINs (below 19 chars)
  -- 19-char NINs are accepted as valid NIDA numbers
  AND NOT (
      UPPER(TRIM(idt.description)) = 'NATIONAL IDENTITY CARD'
      AND LENGTH(TRIM(id.id_no)) < 19
  )

  -- Exclude 19-char values that are clearly not NINs
  -- (T-prefix voter IDs, council references with slashes, spaces)
  AND NOT (
      UPPER(TRIM(idt.description)) = 'NATIONAL IDENTITY CARD'
      AND LENGTH(TRIM(id.id_no)) = 19
      AND (
          TRIM(id.id_no) LIKE 'T%'
       OR TRIM(id.id_no) LIKE '%/%'
       OR TRIM(id.id_no) LIKE '% %'
      )
  )

  -- Driving licence length validation
  AND NOT (
      UPPER(TRIM(idt.description)) = 'DRIVING LICENCE'
      AND LENGTH(TRIM(id.id_no)) < 10
  )

  -- Voter ID format validation
  AND NOT (
      UPPER(TRIM(idt.description)) = 'VOTERS ID'
      AND UPPER(TRIM(id.id_no)) NOT LIKE 'T%'
  );