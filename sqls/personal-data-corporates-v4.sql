
WITH corporate_customers AS
         (SELECT CUST_ID,
                 ID_NO
          FROM (SELECT CUST_ID,
                       ID_NO,
                       ROW_NUMBER() OVER
                           (
                           PARTITION BY CUST_ID
                           ORDER BY CUST_ID
                           ) AS rn
                FROM W_DIM_CUSTOMER
                WHERE CUST_TYPE_IND = 'Corporate') x
          WHERE rn = 1),

     district_wards AS (SELECT DISTINCT DISTRICT,
                                        WARD,
                                        ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY WARD) AS rn,
                                        COUNT(*) OVER (PARTITION BY DISTRICT)                   AS total_wards
                        FROM bank_location_lookup_v2)
        ,
     region_districts AS (SELECT REGION,
                                 DISTRICT,
                                 ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT) AS rn,
                                 COUNT(*) OVER (PARTITION BY REGION)                       AS total_districts
                          FROM bank_location_lookup_v2
                          GROUP BY REGION, DISTRICT),

     corporate_agreements AS
         (SELECT DISTINCT a.AGR_SN,
                          ca.FK_CUSTOMERCUST_ID AS corporate_cust_id
          FROM AGREEMENT a
                   JOIN CUST_ADDRESS ca
                        ON a.FK_CUST_ADDRESSFK = ca.FK_CUSTOMERCUST_ID
                            AND a.FK_CUST_ADDRESSSER = ca.SERIAL_NUM

                   JOIN corporate_customers cc
                        ON cc.CUST_ID = ca.FK_CUSTOMERCUST_ID

          WHERE EXISTS
                    (SELECT 1
                     FROM PROFITS_ACCOUNT pa
                     WHERE pa.CUST_ID = ca.FK_CUSTOMERCUST_ID
                       AND pa.PRODUCT_ID = 31704))

SELECT CURRENT_TIMESTAMP                               AS reportingDate,
       corp.SURNAME                                    AS companyName,
       ca.corporate_cust_id                            AS customerIdentificationNumber,
       corp.CUST_OPEN_DATE                             AS establishedDate,
       'LimitedLiabilityCompanyPrivate'                AS legalForm,
       'NoNegativeStatus'                              AS negativeClientStatus,
       corp.NO_OF_EMPLOYEES                            AS numberOfEmployees,
       0                                               AS totalEmployeesMAle,
       0                                               AS totalEmployeesFemale,
       CASE UPPER(TRIM(id_country.description))
           WHEN 'TANZANIA'
               THEN 'TANZANIA, UNITED REPUBLIC OF'
           ELSE 'TANZANIA, UNITED REPUBLIC OF'
           END                                         AS registrationCountry,
       id.id_no                                        AS registrationNumber,
       cc.ID_NO                                        AS taxIdentificationNumber,
       corp.SURNAME                                    AS tradeName,
       NULL                                            AS parentName,
       NULL                                            AS parentIncorporationNumber,
       NULL                                            AS groupId,
       'Other financial Corporations'                  AS sectorSnaClassification,

       '[' ||
       RTRIM(
               CAST(
                       XMLSERIALIZE(
                               XMLAGG(
                                       XMLTEXT(
                                               '{' ||
                                               '"fullName":"' || REPLACE(COALESCE(TRIM(rel.FIRST_NAME) || ' ' ||
                                                                                  COALESCE(TRIM(rel.MIDDLE_NAME) || ' ', '') ||
                                                                                  TRIM(rel.SURNAME), ''), '"', '\"') ||
                                               '",' ||
                                               '"gender":' || CASE TRIM(rel.SEX)
                                                                  WHEN 'M' THEN '"Male"'
                                                                  WHEN 'F' THEN '"Female"'
                                                                  ELSE 'null' END || ',' ||
                                               '"cellPhone":' || CASE
                                                                     WHEN TRIM(c_address.TELEPHONE) IS NOT NULL
                                                                         THEN '"' || REPLACE(TRIM(c_address.TELEPHONE), '"', '\"') || '"'
                                                                     ELSE 'null' END || ',' ||
                                               '"relationType":"Director",' ||
                                               '"nationalId":' || CASE
                                                                      WHEN TRIM(id.ID_NO) IS NOT NULL
                                                                          THEN '"' || REPLACE(TRIM(id.ID_NO), '"', '\"') || '"'
                                                                      ELSE 'null' END || ',' ||
                                               '"nationality":' || CASE UPPER(TRIM(id_country.description))
                                                                       WHEN 'TANZANIA'
                                                                           THEN '"TANZANIA, UNITED REPUBLIC OF"'
                                                                       ELSE 'null' END || ',' ||
                                               '"appointmentDate":"' || corp.CUST_OPEN_DATE || '",' ||
                                               '"terminationDate":null,' ||
                                               '"rateSharesOwnedValue":"N/A",' ||
                                               '"amountSharesOwnedValue":"N/A"' ||
                                               '},'
                                       )
                               ) AS CLOB
                       ) AS VARCHAR(32000)
               ), ','
       ) ||
       ']'                                             AS related_customers,

       -- BUSINESS ADDRESS
       ward_selection.WARD                             AS street,
       CASE UPPER(TRIM(id_country.description))
           WHEN 'TANZANIA'
               THEN 'TANZANIA, UNITED REPUBLIC OF' END AS country,
       COALESCE(
               loc_region_city.REGION,
               loc_region_dist.REGION,
               loc_region_from_district.REGION,
               loc_region_from_ward.REGION,
               'Dar es Salaam'
       )                                               AS region,
       COALESCE(
               loc_district_region.DISTRICT,
               loc_district_from_ward.DISTRICT,
               loc_district_from_city.DISTRICT,
               loc_district_from_region.DISTRICT
       )                                               AS district,
       ward_selection.WARD                             AS ward,
       NULL                                            AS houseNumber,
       ward_selection.WARD                             AS postalCode,
       c_address.ADDRESS_1                             AS poBox,
       c_address.ZIP_CODE                              AS zipCode,


       -- SECONDARY ADDRESSES
       NULL                                            AS secondaryStreet,
       NULL                                            AS secondartHouseNumber,
       NULL                                            AS secondaryPostalCode,
       NULL                                            AS secondaryRegion,
       NULL                                            AS secondaryDistrict,
       'TANZANIA, UNITED REPUBLIC OF'                  AS secondaryCountry,
       NULL                                            AS secondaryTextAddress,

       --CONTACT PERSON
       NULL                                            AS mobileNumber,
       NULL                                            AS alternativeMobileNumber,
       NULL                                            AS fixedLineNumber,
       NULL                                            AS faxNumber,
       NULL                                            AS emailAddress,
       NULL                                            AS socialMedia,

       -- RELATIONS ENTITY
       'N/A'                                           AS entityName,
       'N/A'                                           AS entityType,
       NULL                                            AS certificateIncorporation,
       COALESCE(
               loc_region_city.REGION,
               loc_region_dist.REGION,
               loc_region_from_district.REGION,
               loc_region_from_ward.REGION,
               'Dar es Salaam'
       )                                               AS entiryRegion,
       COALESCE(
               loc_district_region.DISTRICT,
               loc_district_from_ward.DISTRICT,
               loc_district_from_city.DISTRICT,
               loc_district_from_region.DISTRICT
       )                                               AS entityDistrict,
       'N/A'                                           AS entityWard,
       'N/A'                                           AS entityStreet,
       'N/A'                                           AS entityHouseNumber,
       'N/A'                                           AS entityPostalCode,
       'N/A'                                           AS groupParentCode,
       NULL                                            AS shareOwnedPercentage,
       NULL                                            AS shareOwnedAmount

FROM corporate_agreements ca

         JOIN PROFITS.CUSTOMER corp
              ON corp.CUST_ID = ca.corporate_cust_id


         LEFT JOIN cust_address c_address
                   ON c_address.fk_customercust_id = corp.cust_id
                       AND c_address.communication_addr = '1'
                       AND c_address.entry_status = '1'

    --lookup
         LEFT JOIN (SELECT REGION,
                           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_region_birth_city
                   ON REPLACE(
                              REPLACE(
                                      REPLACE(
                                              REPLACE(UPPER(TRIM(corp.BIRTHPLACE)), ' ', ''),
                                              '-', ''),
                                      '_', ''),
                              ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_region_birth_city.REGION)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                          || '%'
                       AND loc_region_birth_city.rn = 1

    -- fallback on district to region
         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_birth_region_from_district
                   ON loc_region_birth_city.REGION IS NULL
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(corp.BIRTHPLACE)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_birth_region_from_district.DISTRICT)),
                                                               ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
                       AND loc_birth_region_from_district.rn = 1

-- fallback on ward
         LEFT JOIN (SELECT REGION,
                           WARD,
                           ROW_NUMBER() OVER (PARTITION BY WARD ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_birth_region_from_ward
                   ON loc_region_birth_city.REGION IS NULL
                       AND loc_birth_region_from_district.REGION IS NULL
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(corp.BIRTHPLACE)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_birth_region_from_ward.WARD)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
                       AND loc_birth_region_from_ward.rn = 1
    --current location lookup

    --current location lookup
    --region
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

    --fallback to district then take the region
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
    --no fallback
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
-- fallback to take random district from the ward
         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT ) AS rn
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

         LEFT JOIN district_wards ward_selection
                   ON ward_selection.DISTRICT = COALESCE(
                           loc_district_region.DISTRICT,
                           loc_district_from_ward.DISTRICT,
                           loc_district_from_city.DISTRICT,
                           loc_district_from_region.DISTRICT,
                           'Dar es Salaam'
                                                )
                       AND
                      ward_selection.rn = MOD(ASCII(SUBSTR(TRIM(corp.CUST_ID), 1, 1)), ward_selection.total_wards) + 1
    -- end of district mapping


         LEFT JOIN region_districts birth_district_pick
                   ON birth_district_pick.REGION =
                      COALESCE(
                              loc_region_birth_city.REGION,
                              loc_birth_region_from_district.REGION,
                              loc_birth_region_from_ward.REGION,
                              loc_region_city.REGION,
                              loc_region_dist.REGION,
                              loc_region_from_district.REGION,
                              loc_region_from_ward.REGION,
                              'Dar es Salaam'
                      )
                       AND birth_district_pick.rn =
                           MOD(
                                   ASCII(SUBSTR(TRIM(corp.CUST_ID), 1, 1)),
                                   birth_district_pick.total_districts
                           ) + 1
    -- end of mapping
         JOIN corporate_customers cc
              ON cc.CUST_ID = corp.CUST_ID

         JOIN AGREEMENT a2
              ON a2.AGR_SN = ca.AGR_SN

         JOIN CUST_ADDRESS ca2
              ON a2.FK_CUST_ADDRESSFK = ca2.FK_CUSTOMERCUST_ID
                  AND a2.FK_CUST_ADDRESSSER = ca2.SERIAL_NUM

         JOIN CUSTOMER rel
              ON rel.CUST_ID = ca2.FK_CUSTOMERCUST_ID

    --          LEFT JOIN cust_address c_address
--                    ON c_address.fk_customercust_id = corp.cust_id
--                        AND c_address.communication_addr = '1'
--                        AND c_address.entry_status = '1'

         LEFT JOIN other_id id
                   ON id.fk_customercust_id = corp.cust_id
                       AND (CASE
                                WHEN id.serial_no IS NULL
                                    THEN '1'
                                ELSE id.main_flag
                           END) = '1'

         LEFT JOIN generic_detail id_country
                   ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                       AND id.fkgd_has_been_issu = id_country.serial_num

WHERE ca2.FK_CUSTOMERCUST_ID <> ca.corporate_cust_id
  AND rel.FIRST_NAME IS NOT NULL
  AND TRIM(rel.FIRST_NAME) <> ''

GROUP BY corp.SURNAME,
         ca.corporate_cust_id,
         corp.CUST_OPEN_DATE,
         corp.CITY_OF_BIRTH,
         loc_district_region.DISTRICT,
         loc_district_from_ward.DISTRICT,
         loc_district_from_city.DISTRICT,
         loc_district_from_region.DISTRICT,
         loc_region_city.REGION,
         loc_region_dist.REGION,
         loc_region_from_district.REGION,
         loc_region_from_ward.REGION,
         id_country.DESCRIPTION,
         ward_selection.WARD,
         c_address.ZIP_CODE,
         c_address.ADDRESS_1,
         corp.NO_OF_EMPLOYEES,
         id.ID_NO,
         cc.ID_NO

ORDER BY corp.SURNAME;