SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')    AS reportingDate,
       201                                                  AS posBranchCode,
       at.FK_USRCODE                                        AS posNumber,
       'FSR-' || CAST(at.FK_USRCODE AS VARCHAR(10))         AS qrFsrCode,
       'Bank Agent'                                         AS posHolderCategory,
       'Selcom'                                             AS posHolderName,
       null                                                 AS posHolderNin,
       '103-847-451'                                        AS posHolderTin,
       NULL                                                 AS postalCode,
       COALESCE(region_lkp.BOT_REGION, 'N/A')               AS region,
       COALESCE(district_lkp.BOT_DISTRICT, 'N/A')           AS district,
       COALESCE(ward_lkp.BOT_WARD, 'N/A')                   AS ward,
       'N/A'                                                AS street,
       'N/A'                                                AS houseNumber,
       al.GPS                                               AS gpsCoordinates,
       '230000070'                                          AS linkedAccount,
       VARCHAR_FORMAT(at.INSERTION_TMSTAMP, 'DDMMYYYYHHMM') AS issueDate,
       NULL                                                 AS returnDate
FROM AGENT_TERMINAL at
         JOIN (SELECT DISTINCT RIGHT(TRIM(TERMINAL_ID), 8) AS TERMINAL_ID_8, gps
               FROM AGENTS_LIST) al
              ON al.TERMINAL_ID_8 = TRIM(at.FK_USRCODE)
         LEFT JOIN (SELECT TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8)) AS TERMINAL_KEY,
                           bl.REGION                             AS BOT_REGION,
                           ROW_NUMBER() OVER (
                               PARTITION BY TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8))
                               ORDER BY
                                   CASE
                                       WHEN UPPER(TRIM(al.REGION)) = UPPER(TRIM(bl.REGION)) THEN 1
                                       WHEN UPPER(TRIM(al.REGION)) LIKE UPPER(TRIM(bl.REGION)) || '%' THEN 2
                                       ELSE 99
                                       END,
                                   LENGTH(TRIM(bl.REGION)) DESC
                               )                                 AS rn
                    FROM AGENTS_LIST al
                             JOIN BANK_LOCATION_LOOKUP_V2 bl
                                  ON UPPER(TRIM(al.REGION)) = UPPER(TRIM(bl.REGION))
                                      OR (
                                         UPPER(TRIM(al.REGION)) LIKE UPPER(TRIM(bl.REGION)) || '%'
                                             AND LENGTH(TRIM(bl.REGION)) >= 4
                                         )) region_lkp
                   ON region_lkp.TERMINAL_KEY = TRIM(at.FK_USRCODE)
                       AND region_lkp.rn = 1

         LEFT JOIN (SELECT TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8)) AS TERMINAL_KEY,
                           bl.DISTRICT                           AS BOT_DISTRICT,
                           ROW_NUMBER() OVER (
                               PARTITION BY TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8))
                               ORDER BY
                                   CASE
                                       -- 1️⃣ Exact match
                                       WHEN UPPER(TRIM(al.DISTRICT)) = UPPER(TRIM(bl.DISTRICT)) THEN 1

                                       -- 2️⃣ Safe starts-with
                                       WHEN UPPER(TRIM(al.DISTRICT)) LIKE UPPER(TRIM(bl.DISTRICT)) || '%'
                                           AND LENGTH(TRIM(bl.DISTRICT)) >= 4 THEN 2

                                       -- 3️⃣ No random fallback
                                       ELSE 99
                                       END,
                                   LENGTH(TRIM(bl.DISTRICT)) DESC
                               )                                 AS rn
                    FROM AGENTS_LIST al
                             JOIN BANK_LOCATION_LOOKUP_V2 bl
                                  ON (
                                      UPPER(TRIM(al.DISTRICT)) = UPPER(TRIM(bl.DISTRICT))
                                          OR (
                                          UPPER(TRIM(al.DISTRICT)) LIKE UPPER(TRIM(bl.DISTRICT)) || '%'
                                              AND LENGTH(TRIM(bl.DISTRICT)) >= 4
                                          )
                                      )
                    WHERE TRIM(al.DISTRICT) IS NOT NULL
                      AND TRIM(al.DISTRICT) <> '') district_lkp
                   ON district_lkp.TERMINAL_KEY = TRIM(at.FK_USRCODE)
                       AND district_lkp.rn = 1


         LEFT JOIN (SELECT TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8)) AS TERMINAL_KEY,
                           bl.WARD                               AS BOT_WARD,
                           ROW_NUMBER() OVER (
                               PARTITION BY TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8))
                               ORDER BY
                                   CASE
                                       -- 1️⃣ Exact match
                                       WHEN UPPER(TRIM(al.LOCATION)) = UPPER(TRIM(bl.WARD)) THEN 1

                                       -- 2️⃣ Safe starts-with
                                       WHEN UPPER(TRIM(al.LOCATION)) LIKE UPPER(TRIM(bl.WARD)) || '%'
                                           AND LENGTH(TRIM(bl.WARD)) >= 4 THEN 2

                                       -- 3️⃣ No random fallback
                                       ELSE 99
                                       END,
                                   LENGTH(TRIM(bl.WARD)) DESC
                               )                                 AS rn
                    FROM AGENTS_LIST al
                             JOIN BANK_LOCATION_LOOKUP_V2 bl
                                  ON (
                                      UPPER(TRIM(al.LOCATION)) = UPPER(TRIM(bl.WARD))
                                          OR (
                                          UPPER(TRIM(al.LOCATION)) LIKE UPPER(TRIM(bl.WARD)) || '%'
                                              AND LENGTH(TRIM(bl.WARD)) >= 4
                                          )
                                      )
                    WHERE TRIM(al.LOCATION) IS NOT NULL
                      AND TRIM(al.LOCATION) <> '') ward_lkp
                   ON ward_lkp.TERMINAL_KEY = TRIM(at.FK_USRCODE)
                       AND ward_lkp.rn = 1;