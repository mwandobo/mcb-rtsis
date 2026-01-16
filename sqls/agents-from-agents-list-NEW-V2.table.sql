SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                                            AS reportingDate,
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
       )                                                                                            AS agentName,
       al.AGENT_ID                                                                                  AS agentId,
       null                                                                                         AS tillNumber,
       CASE
           WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'SOLE PROPRIETORY' THEN 'Sole Proprietor'
           WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'LIMITED COMPANY' THEN 'Company'
           WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'PRIVATE COMPANY' THEN 'Company'
           WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'CO-OPERATIVE SOCIETY' THEN 'Trust'
           WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'PARTNERSHIP' THEN 'Partnership'
           ELSE TRIM(al.BUSINESS_FORM)
           END                                                                                      AS businessForm,
       'bank'                                                                                       AS agentPrincipal,
       'Selcom'                                                                                     AS agentPrincipalName,
       CASE WHEN be.SEX = 'M' then 'Male' WHEN be.SEX = 'F' then 'female' ELSE 'Not Applicable' END AS gender,
       VARCHAR_FORMAT(COALESCE(be.TMSTAMP, CURRENT_DATE), 'DDMMYYYYHHMM')                           AS registrationDate,
       null                                                                                         AS closedDate,
       al.CERT_IN_CORPORATION                                                                       AS certIncorporation,
       'TANZANIA, UNITED REPUBLIC OF'                                                               AS nationality,
       CASE
           WHEN be.EMPL_STATUS = '1' THEN 'Active'
           WHEN be.EMPL_STATUS = '0' THEN 'Inactive'
           ELSE 'Suspended'
           END                                                                                      AS agentStatus,
       'super agent'                                                                                AS agentType,
       null                                                                                         AS accountNumber,
       COALESCE(region_lkp.BOT_REGION, 'N/A')                                            AS region,
       COALESCE(district_lkp.BOT_DISTRICT,  'N/A')                                      AS district,
       COALESCE(ward_lkp.BOT_WARD,  'N/A')                                              AS ward,
       'N/A'                                                                                        AS street,
       'N/A'                                                                                        AS houseNumber,
       'N/A'                                                                                        AS postalCode,
       'TANZANIA, UNITED REPUBLIC OF'                                                               AS country,
       al.GPS                                                                                       AS gpsCoordinates,
       al.TIN                                                                                       AS agentTaxIdentificationNumber,
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
           END                                                                                      AS businessLicense
FROM AGENTS_LIST al
         LEFT JOIN BANKEMPLOYEE be
                   ON RIGHT(TRIM(al.TERMINAL_ID), 8) = TRIM(be.STAFF_NO)
         LEFT JOIN (SELECT al.AGENT_ID,
                           bl.REGION AS BOT_REGION,
                           ROW_NUMBER() OVER (
                               PARTITION BY al.AGENT_ID
                               ORDER BY
                                   CASE
                                       WHEN UPPER(TRIM(al.REGION)) = UPPER(TRIM(bl.REGION)) THEN 1 -- exact
                                       WHEN UPPER(TRIM(al.REGION)) LIKE UPPER(TRIM(bl.REGION)) || '%'
                                           THEN 2 -- safe starts-with
                                       ELSE 99 -- do not allow random fallback
                                       END,
                                   LENGTH(TRIM(bl.REGION)) DESC
                               )     AS rn
                    FROM AGENTS_LIST al
                             JOIN BANK_LOCATION_LOOKUP_V2 bl
                                  ON UPPER(TRIM(al.REGION)) = UPPER(TRIM(bl.REGION))
                                      OR (UPPER(TRIM(al.REGION)) LIKE UPPER(TRIM(bl.REGION)) || '%' AND
                                          LENGTH(TRIM(bl.REGION)) >= 4)) region_lkp
                   ON region_lkp.AGENT_ID = al.AGENT_ID
                       AND region_lkp.rn = 1

         LEFT JOIN (SELECT al.AGENT_ID,
                           bl.DISTRICT AS BOT_DISTRICT,
                           ROW_NUMBER() OVER (
                               PARTITION BY al.AGENT_ID
                               ORDER BY
                                   CASE
                                       -- 1️⃣ Exact match
                                       WHEN UPPER(TRIM(al.DISTRICT)) = UPPER(TRIM(bl.DISTRICT)) THEN 1

                                       -- 2️⃣ Starts-with match (safe)
                                       WHEN UPPER(TRIM(al.DISTRICT)) LIKE UPPER(TRIM(bl.DISTRICT)) || '%'
                                           AND LENGTH(TRIM(bl.DISTRICT)) >= 4 THEN 2

                                       -- 3️⃣ No fallback
                                       ELSE 99
                                       END,
                                   LENGTH(TRIM(bl.DISTRICT)) DESC
                               )       AS rn
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
                   ON district_lkp.AGENT_ID = al.AGENT_ID
                       AND district_lkp.rn = 1
         LEFT JOIN (SELECT al.AGENT_ID,
                           bl.WARD AS BOT_WARD,
                           ROW_NUMBER() OVER (
                               PARTITION BY al.AGENT_ID
                               ORDER BY
                                   CASE
                                       -- 1️⃣ Exact match
                                       WHEN UPPER(TRIM(al.LOCATION)) = UPPER(TRIM(bl.WARD)) THEN 1

                                       -- 2️⃣ Starts-with match (safe)
                                       WHEN UPPER(TRIM(al.LOCATION)) LIKE UPPER(TRIM(bl.WARD)) || '%'
                                           AND LENGTH(TRIM(bl.WARD)) >= 4 THEN 2

                                       -- 3️⃣ No fallback
                                       ELSE 99
                                       END,
                                   LENGTH(TRIM(bl.WARD)) DESC
                               )   AS rn
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
                   ON ward_lkp.AGENT_ID = al.AGENT_ID
                       AND ward_lkp.rn = 1

-- FETCH FIRST 10 ROWS ONLY
;
