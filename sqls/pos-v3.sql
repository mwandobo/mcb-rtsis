SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')    AS reportingDate,
       201                                                  AS posBranchCode,
       at.FK_USRCODE                                        AS posNumber,
       'FSR-' || CAST(at.FK_USRCODE AS VARCHAR(10))         AS qrFsrCode,
       'Bank Agent'                                         AS posHolderCategory,
       'Selcom'                                             AS posHolderName,
       NULL                                                 AS posHolderNin,
       '103-847-451'                                        AS posHolderTin,
       NULL                                                 AS postalCode,
--        al.REGION                                            AS region,
--        al.DISTRICT                                          AS district,
--        al.WARD                                              AS ward,
       loc_region.REGION                                 AS region,
       COALESCE(
               loc_district.DISTRICT,
               loc_district_from_region.DISTRICT
       )                                                 AS district,
       COALESCE(
               loc_ward.WARD,
               loc_ward_from_district.WARD
       )                                                 AS ward,
       'N/A'                                                AS street,
       'N/A'                                                AS houseNumber,
       COALESCE(al.GPS_COORDINATES, '-6.7725°,38.9769°')    AS gpsCoordinates,
       '230000070'                                          AS linkedAccount,
       VARCHAR_FORMAT(at.INSERTION_TMSTAMP, 'DDMMYYYYHHMM') AS issueDate,
       NULL                                                 AS returnDate
FROM AGENT_TERMINAL at
         LEFT JOIN AGENTS_LIST_V3 al
                   ON
                       CASE
                           WHEN LENGTH(REPLACE(at.FK_USRCODE, ' ', '')) > 8
                               THEN RIGHT(REPLACE(at.FK_USRCODE, ' ', ''), 8)
                           ELSE REPLACE(at.FK_USRCODE, ' ', '')
                           END
                           =
                       CASE
                           WHEN LENGTH(REPLACE(al.TERMINAL_ID, ' ', '')) > 8
                               THEN RIGHT(REPLACE(al.TERMINAL_ID, ' ', ''), 8)
                           ELSE REPLACE(al.TERMINAL_ID, ' ', '')
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
