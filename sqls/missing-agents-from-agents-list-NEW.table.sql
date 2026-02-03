WITH agent_terminals_enriched AS (SELECT at.*,
                                         TRIM(
                                                 CAST(TRIM(COALESCE(be.FIRST_NAME, '')) AS VARCHAR(100)) ||
                                                 CASE
                                                     WHEN TRIM(COALESCE(be.FATHER_NAME, '')) <> ''
                                                         THEN ' ' || CAST(TRIM(be.FATHER_NAME) AS VARCHAR(100))
                                                     ELSE ''
                                                     END ||
                                                 CASE
                                                     WHEN TRIM(COALESCE(be.LAST_NAME, '')) <> ''
                                                         THEN ' ' || CAST(TRIM(be.LAST_NAME) AS VARCHAR(100))
                                                     ELSE ''
                                                     END
                                         ) AS agentName,
                                         CHAR(
                                                 max_ids.max_agent_id
                                                     + ROW_NUMBER() OVER (ORDER BY at.USER_CODE)
                                         ) AS new_agent_id


                                  FROM AGENT_TERMINAL at
                                           CROSS JOIN (SELECT MAX(INTEGER(TRIM(AGENT_ID))) AS max_agent_id
                                                       FROM AGENTS_LIST
                                                       WHERE AGENT_ID IS NOT NULL
                                                         AND TRIM(AGENT_ID) <> ''
                                                         AND TRANSLATE(AGENT_ID, '', '0123456789') = '') max_ids
                                           LEFT JOIN BANKEMPLOYEE be
                                                     ON be.STAFF_NO = at.USER_CODE)




SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       ate.agentName                                     AS agentName,
       ate.new_agent_id                                  AS agentId,
       ate.USER_CODE                                     AS TERMINALiD,
       null                                              AS tillNumber,
       al.BUSINESS_FORM                                  AS businessForm,
       'bank'                                            AS agentPrincipal,
       'Selcom'                                          AS agentPrincipalName,
       null                                              AS closedDate,
       al.CERT_IN_CORPORATION                            AS certIncorporation,
       'TANZANIA, UNITED REPUBLIC OF'                    AS nationality,
--        region_lkp.BOT_REGION                                           AS region,
--        district_lkp.BOT_DISTRICT                                    AS district,
--   ward_lkp.BOT_WARD                                            AS ward,
       null                                              AS accountNumber,
       al.REGION                                         AS region,
       al.DISTRICT                                       AS district,
       AL.LOCATION                                       AS ward,
       null                                              AS street,
       null                                              AS houseNumber,
       null                                              AS postalCode,
       'TANZANIA, UNITED REPUBLIC OF'                    AS country,
       al.GPS                                            AS gpsCoordinates,
       al.TIN                                            AS agentTaxIdentificationNumber,
       CASE
           -- 1️⃣ Comma exists
           WHEN LOCATE(',', al.BUSINESS_LICENCE_ISSUER_AND_DATE) > 0 THEN
               CASE
                   -- 1a. If there's a space before the comma, use first word (up to first space)
                   WHEN LOCATE(' ', SUBSTR(al.BUSINESS_LICENCE_ISSUER_AND_DATE, 1,
                                           LOCATE(',', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1)) > 0 THEN
                       TRIM(
                               SUBSTR(
                                       al.BUSINESS_LICENCE_ISSUER_AND_DATE,
                                       1,
                                       LOCATE(' ', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1
                               )
                       )
                   -- 1b. No space before comma, use everything before comma
                   ELSE
                       TRIM(
                               SUBSTR(
                                       al.BUSINESS_LICENCE_ISSUER_AND_DATE,
                                       1,
                                       LOCATE(',', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1
                               )
                       )
                   END
           -- 2️⃣ No comma, but space exists
           WHEN LOCATE(' ', al.BUSINESS_LICENCE_ISSUER_AND_DATE) > 0 THEN
               TRIM(
                       SUBSTR(
                               al.BUSINESS_LICENCE_ISSUER_AND_DATE,
                               1,
                               LOCATE(' ', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1
                       )
               )
           -- 3️⃣ Last resort: use whole string
           ELSE TRIM(al.BUSINESS_LICENCE_ISSUER_AND_DATE)
           END                                           AS businessLicense

FROM agent_terminals_enriched ate
         LEFT JOIN AGENTS_LIST al
                   ON RIGHT(TRIM(al.TERMINAL_ID), 8) = TRIM(ate.USER_CODE)

--          LEFT JOIN (SELECT al.AGENT_ID,
--                            bl.REGION AS BOT_REGION,
--                            ROW_NUMBER() OVER (
--                                PARTITION BY al.AGENT_ID
--                                ORDER BY
--                                    CASE
--                                        WHEN UPPER(TRIM(al.REGION)) = UPPER(TRIM(bl.REGION)) THEN 1 -- exact
--                                        WHEN UPPER(TRIM(al.REGION)) LIKE UPPER(TRIM(bl.REGION)) || '%'
--                                            THEN 2 -- safe starts-with
--                                        ELSE 99 -- do not allow random fallback
--                                        END,
--                                    LENGTH(TRIM(bl.REGION)) DESC
--                                )     AS rn
--                     FROM AGENTS_LIST al
--                              JOIN BANK_LOCATION_LOOKUP_V2 bl
--                                   ON UPPER(TRIM(al.REGION)) = UPPER(TRIM(bl.REGION))
--                                       OR (UPPER(TRIM(al.REGION)) LIKE UPPER(TRIM(bl.REGION)) || '%' AND
--                                           LENGTH(TRIM(bl.REGION)) >= 4)) region_lkp
--                    ON region_lkp.AGENT_ID = al.AGENT_ID
--                        AND region_lkp.rn = 1
--
--          LEFT JOIN (SELECT al.AGENT_ID,
--                            bl.DISTRICT AS BOT_DISTRICT,
--                            ROW_NUMBER() OVER (
--                                PARTITION BY al.AGENT_ID
--                                ORDER BY
--                                    CASE
--                                        -- 1️⃣ Exact match
--                                        WHEN UPPER(TRIM(al.DISTRICT)) = UPPER(TRIM(bl.DISTRICT)) THEN 1
--
--                                        -- 2️⃣ Starts-with match (safe)
--                                        WHEN UPPER(TRIM(al.DISTRICT)) LIKE UPPER(TRIM(bl.DISTRICT)) || '%'
--                                            AND LENGTH(TRIM(bl.DISTRICT)) >= 4 THEN 2
--
--                                        -- 3️⃣ No fallback
--                                        ELSE 99
--                                        END,
--                                    LENGTH(TRIM(bl.DISTRICT)) DESC
--                                )       AS rn
--                     FROM AGENTS_LIST al
--                              JOIN BANK_LOCATION_LOOKUP_V2 bl
--                                   ON (
--                                       UPPER(TRIM(al.DISTRICT)) = UPPER(TRIM(bl.DISTRICT))
--                                           OR (
--                                           UPPER(TRIM(al.DISTRICT)) LIKE UPPER(TRIM(bl.DISTRICT)) || '%'
--                                               AND LENGTH(TRIM(bl.DISTRICT)) >= 4
--                                           )
--                                       )
--                     WHERE TRIM(al.DISTRICT) IS NOT NULL
--                       AND TRIM(al.DISTRICT) <> '') district_lkp
--                    ON district_lkp.AGENT_ID = al.AGENT_ID
--                        AND district_lkp.rn = 1
--          LEFT JOIN (SELECT al.AGENT_ID,
--                            bl.WARD AS BOT_WARD,
--                            ROW_NUMBER() OVER (
--                                PARTITION BY al.AGENT_ID
--                                ORDER BY
--                                    CASE
--                                        -- 1️⃣ Exact match
--                                        WHEN UPPER(TRIM(al.LOCATION)) = UPPER(TRIM(bl.WARD)) THEN 1
--
--                                        -- 2️⃣ Starts-with match (safe)
--                                        WHEN UPPER(TRIM(al.LOCATION)) LIKE UPPER(TRIM(bl.WARD)) || '%'
--                                            AND LENGTH(TRIM(bl.WARD)) >= 4 THEN 2
--
--                                        -- 3️⃣ No fallback
--                                        ELSE 99
--                                        END,
--                                    LENGTH(TRIM(bl.WARD)) DESC
--                                )   AS rn
--                     FROM AGENTS_LIST al
--                              JOIN BANK_LOCATION_LOOKUP_V2 bl
--                                   ON (
--                                       UPPER(TRIM(al.LOCATION)) = UPPER(TRIM(bl.WARD))
--                                           OR (
--                                           UPPER(TRIM(al.LOCATION)) LIKE UPPER(TRIM(bl.WARD)) || '%'
--                                               AND LENGTH(TRIM(bl.WARD)) >= 4
--                                           )
--                                       )
--                     WHERE TRIM(al.LOCATION) IS NOT NULL
--                       AND TRIM(al.LOCATION) <> '') ward_lkp
--                    ON ward_lkp.AGENT_ID = al.AGENT_ID
--                        AND ward_lkp.rn = 1

WHERE al.TERMINAL_ID IS NULL
ORDER BY ate.USER_CODE
