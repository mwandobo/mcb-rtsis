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
           WHEN 'MARRIED' THEN 'Married'
           WHEN 'SINGLE' THEN 'Single'
           WHEN 'DIVORCED' THEN 'Divorced'
           WHEN 'WIDOWED' THEN 'Widowed'
           ELSE 'Single'
           END                                                                                          AS maritalStatus,
       NULL                                                                                             AS numberSpouse,
       CASE UPPER(TRIM(gd_natio.description)) WHEN 'TANZANIAN' THEN 'TANZANIA, UNITED REPUBLIC OF' END  AS nationality,
       CASE UPPER(TRIM(gd_natio.description)) WHEN 'TANZANIAN' THEN 'TANZANIA, UNITED REPUBLIC OF' END  AS citizenship,
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
           WHEN 'EMPLOYED' THEN 'Employed'
           WHEN 'SALARIED' THEN 'Employed'
           WHEN 'CUSTOMER SERVICE' THEN 'Self-employed'
           ELSE 'Unemployed'
           END                                                                                          AS employmentStatus,
       gd_customer_income.DESCRIPTION                                                                   AS monthlyIncome,
       (c.num_of_children + c.children_above18)                                                         AS numberDependants,
       gd_edulevel.description                                                                          AS educationLevel,
       0.00                                                                                             AS averageMonthlyExpenditure,
       c.blacklisted_ind                                                                                AS negativeClientStatus,
       c.spouse_name                                                                                    AS spousesFullName,
       NULL                                                                                             AS spouseIdentificationType,
       NULL                                                                                             AS spouseIdentificationNumber,
       NULL                                                                                             AS maidenName,
       NULL                                                                                             AS monthlyExpenses,
       c.date_of_birth                                                                                  AS birthDate,
       id_country.description                                                                           AS birthCountry,
       CASE UPPER(TRIM(id_country.description)) WHEN 'TANZANIA' THEN 'TANZANIA, UNITED REPUBLIC OF' END AS birthCountry,
       NULL                                                                                             AS birthPostalCode,
       NULL                                                                                             AS birthHouseNumber,
       C.BIRTHPLACE                                                                                     AS birthRegion,
       'N/A'                                                                                            AS birthDistrict,
       NULL                                                                                             AS birthWard,
       NULL                                                                                             AS birthStreet,
       CASE UPPER(TRIM(idt.description))
           WHEN 'COMPANYS REGISTRY NUMBER' THEN 'Certificate of Registration'
           WHEN 'DRIVING LICENCE' THEN 'DrivingLicense'
           WHEN 'NATIONAL IDENTITY CARD' THEN 'NationalIdentityCard'
           WHEN 'PASSPORT' THEN 'Passport'
           WHEN 'STUDENT ID' THEN 'Student ID'
           WHEN 'VOTERS ID' THEN 'VotersRegistrationCard'
           ELSE 'N/A'
           END                                                                                          AS identificationType,
       id.id_no                                                                                         AS identificationNumber,
       CASE
           WHEN id.issue_date = DATE '0001-01-01'
               THEN 'N/A'
           ELSE TO_CHAR(id.issue_date, 'YYYY-MM-DD')
           END                                                                                          AS issuance_date,
       CASE
           WHEN id.expiry_date = DATE '0001-01-01'
               THEN 'N/A'
           ELSE TO_CHAR(id.expiry_date, 'YYYY-MM-DD')
           END                                                                                          AS expirationDate,
       'N/A'                                                                                            AS issuancePlace,

       CASE UPPER(TRIM(idt.description))
           WHEN 'COMPANYS REGISTRY NUMBER' THEN 'Business Registrations and Licensing Agency (BRELA)'
           WHEN 'DRIVING LICENCE' THEN 'Tanzania Revenue Authority (TRA)'
           WHEN 'NATIONAL IDENTITY CARD' THEN 'National Identification Authority (NIDA)'
           WHEN 'PASSPORT' THEN 'Immigration Services Department'
           WHEN 'STUDENT ID' THEN 'Recognized Education Institution'
           WHEN 'VOTERS ID' THEN 'Independent National Electoral Commission (INEC)'
           END                                                                                          AS issuingAuthority,
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
       c_address.address_1 || ' ' || c_address.address_2                                                AS mainAddress,
       NULL                                                                                             AS street,
       NULL                                                                                             AS houseNumber,
       c_address.zip_code                                                                               AS postalCode,
       c_address.CITY                                                                                   AS region,
--        loc_region_city.REGION                                                                           AS bot_region,
--        COALESCE(
--                loc_region_city.REGION,
--                loc_region_dist.REGION
--        )                                                                                                AS bot_region_v1,
--        COALESCE(
--                loc_region_city.REGION,
--                loc_region_dist.REGION,
--                loc_region_from_district.REGION
--        )                                                                                                AS bot_region_v2,
       COALESCE(
               loc_region_city.REGION,
               loc_region_dist.REGION,
               loc_region_from_district.REGION,
               loc_region_from_ward.REGION,
               'Dar es Salaam'
       )                                                                                                AS bot_region_v3,
--        c_address.REGION                                                                                 AS district,

       COALESCE(
               loc_district_region.DISTRICT,
               loc_district_from_ward.DISTRICT,
               loc_district_from_city.DISTRICT,
               loc_district_from_region.DISTRICT
       )                                                                                                AS bot_district_v2,
       c_address.ADDRESS_1                                                                              AS ward,
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


    --region join and lookups
    --no fallback
         LEFT JOIN (SELECT REGION,
                           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_region_city
                   ON REPLACE(
                              REPLACE(
                                      REPLACE(
                                              REPLACE(UPPER(TRIM(c_address.CITY)), ' ', ''),
                                              '-', ''),
                                      '_', ''),
                              ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_region_city.REGION)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                          || '%'
                       AND loc_region_city.rn = 1

-- fallback on district to region
         LEFT JOIN (SELECT REGION,
                           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_region_dist
                   ON loc_region_city.REGION IS NULL
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_region_dist.REGION)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
                       AND loc_region_dist.rn = 1

    --fallback to district to district then take the region
         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_region_from_district
                   ON loc_region_city.REGION IS NULL
                       AND loc_region_dist.REGION IS NULL
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_region_from_district.DISTRICT)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
                       AND loc_region_from_district.rn = 1

    -- fallback to ward then take the region
         LEFT JOIN (SELECT REGION,
                           WARD,
                           ROW_NUMBER() OVER (PARTITION BY WARD ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_region_from_ward
                   ON loc_region_city.REGION IS NULL
                       AND loc_region_dist.REGION IS NULL
                       AND loc_region_from_district.REGION IS NULL
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(c_address.ADDRESS_1)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_region_from_ward.WARD)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
                       AND loc_region_from_ward.rn = 1
    --end of region join and lookups


    --district-mapping

         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) AS rn
                    FROM bank_location_lookup_v2) loc_district_region
                   ON loc_district_region.rn = 1
                       AND COALESCE(
                                   loc_region_city.REGION,
                                   loc_region_dist.REGION,
                                   loc_region_from_district.REGION,
                                   loc_region_from_ward.REGION
                           ) = loc_district_region.REGION
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_district_region.DISTRICT)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'

    -- ward text → district

         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           WARD,
                           ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) AS rn
                    FROM bank_location_lookup_v2) loc_district_from_ward
                   ON loc_district_region.DISTRICT IS NULL
                       AND loc_district_from_ward.rn = 1
                       AND COALESCE(
                                   loc_region_city.REGION,
                                   loc_region_dist.REGION,
                                   loc_region_from_district.REGION,
                                   loc_region_from_ward.REGION
                           ) = loc_district_from_ward.REGION
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(c_address.ADDRESS_1)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_district_from_ward.WARD)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'

         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) AS rn
                    FROM bank_location_lookup_v2) loc_district_from_city
                   ON loc_district_region.DISTRICT IS NULL
                       AND loc_district_from_ward.DISTRICT IS NULL
                       AND loc_district_from_city.rn = 1
                       AND COALESCE(
                                   loc_region_city.REGION,
                                   loc_region_dist.REGION,
                                   loc_region_from_district.REGION,
                                   loc_region_from_ward.REGION
                           ) = loc_district_from_city.REGION
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(c_address.CITY)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_district_from_city.DISTRICT)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT )AS rn
                    FROM bank_location_lookup_v2) loc_district_from_region
                   ON loc_district_region.DISTRICT IS NULL
                       AND loc_district_from_ward.DISTRICT IS NULL
                       AND loc_district_from_city.DISTRICT IS NULL
                       AND loc_district_from_region.rn = 1
                       AND loc_district_from_region.REGION =
                           COALESCE(
                                   loc_region_city.REGION,
                                   loc_region_dist.REGION,
                                   loc_region_from_district.REGION,
                                   loc_region_from_ward.REGION,
                                   'Dar es Salaam'
                           )

    -- end of district mapping

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


WHERE UPPER(TRIM(idt.description)) NOT IN ('OTHER TYPE OF IDENTIFICATION', 'BIRTH CERTIFICATE')
  and c.CUST_TYPE = '1';