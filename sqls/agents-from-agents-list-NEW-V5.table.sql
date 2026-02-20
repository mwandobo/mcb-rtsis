SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       al.AGENT_NAME                                     AS agentName,
       al.AGENT_ID                                       AS agentId,
       al.TILL_NUMBER                                    AS tillNumber,
       COALESCE(al.BUSINESS_FORM, 'Sole Proprietor')     AS businessForm,
       al.AGENT_PRINCIPAL                                AS agentPrincipal,
       al.AGENT_PRINCIPAL_NAME                           AS agentPrincipalName,
       al.GENDER                                         AS gender,
       al.REGISTRATION_DATE                              AS registrationDate,
       al.CLOSED_DATE                                    AS closedDate,
       al.CERT_INCORPORATION                             AS certIncorporation,
       al.NATIONALITY                                    AS nationality,
       CASE
           WHEN be.EMPL_STATUS = '1' THEN 'Active'
           WHEN be.EMPL_STATUS = '0' THEN 'Inactive'
           ELSE 'Suspended'
           END                                           AS agentStatus,
       al.AGENT_TYPE                                     AS agentType,
       al.ACCOUNT_NUMBER                                 AS accountNumber,
       loc_region.REGION                                 AS region,
       COALESCE(
               loc_district.DISTRICT,
               loc_district_from_region.DISTRICT
       )                                                 AS district,
       COALESCE(
               loc_ward.WARD,
               loc_ward_from_district.WARD
       )                                                 AS ward,
       al.STREET                                         AS street,
       al.HOUSE_NUMBER                                   AS houseNumber,
       al.POSTAL_CODE                                    AS postalCode,
       al.COUNTRY                                        AS country,
       COALESCE(al.GPS_COORDINATES, '-6.7725°,38.9769°') AS gpsCoordinates,
       al.AGENT_TAX_IDENTIFICATION_NUMBER                AS agentTaxIdentificationNumber,
       al.BUSINESS_LICENCE                               AS businessLicense
FROM AGENTS_LIST_V4 al
         JOIN BANKEMPLOYEE be
              ON
                  CASE
                      WHEN LENGTH(REPLACE(al.TERMINAL_ID, ' ', '')) > 8
                          THEN RIGHT(REPLACE(al.TERMINAL_ID, ' ', ''), 8)
                      ELSE REPLACE(al.TERMINAL_ID, ' ', '')
                      END
                      =
                  CASE
                      WHEN LENGTH(REPLACE(be.STAFF_NO, ' ', '')) > 8
                          THEN RIGHT(REPLACE(be.STAFF_NO, ' ', ''), 8)
                      ELSE REPLACE(be.STAFF_NO, ' ', '')
                      END
    --location mapping
    --region
         LEFT JOIN (SELECT REGION,
                           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_region
                   ON REPLACE(
                              REPLACE(
                                      REPLACE(
                                              REPLACE(UPPER(TRIM(al.O_REGION)), ' ', ''),
                                              '-', ''),
                                      '_', ''),
                              ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_region.REGION)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                          || '%'
                       AND loc_region.rn = 1
    --end of region

    --district
    --no fallback
         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) AS rn
                    FROM bank_location_lookup_v2) loc_district
                   ON loc_district.rn = 1
                       AND loc_region.REGION = loc_district.REGION
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(al.O_DISTRICT)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_district.DISTRICT)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'

    --fallback to random district from specified region
         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT ) AS rn
                    FROM bank_location_lookup_v2) loc_district_from_region
                   ON loc_district.DISTRICT IS NULL
                       AND loc_district_from_region.rn = 1
                       AND loc_district_from_region.REGION = loc_region.REGION
    --end of district

--ward
--no fallback
         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           WARD,
                           ROW_NUMBER() OVER (PARTITION BY DISTRICT, WARD ORDER BY WARD) AS rn
                    FROM bank_location_lookup_v2) loc_ward
                   ON loc_ward.rn = 1
                       AND loc_region.REGION = loc_ward.REGION
                       AND loc_district.DISTRICT = loc_ward.DISTRICT
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(al.O_WARD)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_ward.WARD)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           WARD,
                           ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY WARD) AS rn
                    FROM bank_location_lookup_v2) loc_ward_from_district
                   ON loc_ward.WARD IS NULL
                       AND loc_ward_from_district.rn = 1
                       AND loc_region.REGION = loc_ward_from_district.REGION
                       AND COALESCE(
                                   loc_district.DISTRICT,
                                   loc_district_from_region.DISTRICT
                           ) = loc_ward_from_district.DISTRICT

--end of district
