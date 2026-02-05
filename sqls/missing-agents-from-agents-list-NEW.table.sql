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
                                         )      AS agentName,
                                      be.STAFF_NO,
                                         be.SEX,
                                         be.TMSTAMP as tms,
                                         be.EMPL_STATUS ,
                                         CHAR(
                                                 max_ids.max_agent_id
                                                     + ROW_NUMBER() OVER (ORDER BY at.USER_CODE)
                                         )      AS new_agent_id


                                  FROM AGENT_TERMINAL at
                                           CROSS JOIN (SELECT MAX(INTEGER(TRIM(AGENT_ID))) AS max_agent_id
                                                       FROM AGENTS_LIST
                                                       WHERE AGENT_ID IS NOT NULL
                                                         AND TRIM(AGENT_ID) <> ''
                                                         AND TRANSLATE(AGENT_ID, '', '0123456789') = '') max_ids
                                           LEFT JOIN BANKEMPLOYEE be
                                                     ON be.STAFF_NO = at.USER_CODE)


SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                                              AS reportingDate,
       ate.agentName                                                                                  AS agentName,
       ate.new_agent_id                                                                               AS agentId,
       ate.STAFF_NO                                                                                   AS TerminalID,
       null                                                                                           AS tillNumber,
       al.BUSINESS_FORM                                                                               AS businessForm,
       'bank'                                                                                         AS agentPrincipal,
       'Selcom'                                                                                       AS agentPrincipalName,
       CASE WHEN ate.SEX = 'M' then 'Male' WHEN ate.SEX = 'F' then 'female' ELSE 'Not Applicable' END AS gender,
       VARCHAR_FORMAT(COALESCE(ate.tms, CURRENT_DATE), 'DDMMYYYYHHMM')                           AS registrationDate,
       null                                                                                           AS closedDate,
       al.CERT_IN_CORPORATION                                                                         AS certIncorporation,
       'TANZANIA, UNITED REPUBLIC OF'                                                                 AS nationality,
--        region_lkp.BOT_REGION                                           AS region,
--        district_lkp.BOT_DISTRICT                                    AS district,
--   ward_lkp.BOT_WARD                                            AS ward,
       CASE
           WHEN ate.EMPL_STATUS = '1' THEN 'Active'
           WHEN ate.EMPL_STATUS = '0' THEN 'Inactive'
           ELSE 'Suspended'
           END                                                                                      AS agentStatus,
       'super agent'                                                                                AS agentType,
       null                                                                                           AS accountNumber,
       al.REGION                                                                                      AS region,
       al.DISTRICT                                                                                    AS district,
       AL.LOCATION                                                                                    AS ward,
       null                                                                                           AS street,
       null                                                                                           AS houseNumber,
       null                                                                                           AS postalCode,
       'TANZANIA, UNITED REPUBLIC OF'                                                                 AS country,
       al.GPS                                                                                         AS gpsCoordinates,
       al.TIN                                                                                         AS agentTaxIdentificationNumber,
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
           END                                                                                        AS businessLicense

FROM agent_terminals_enriched ate
         LEFT JOIN AGENTS_LIST al
                   ON RIGHT(TRIM(al.TERMINAL_ID), 8) = TRIM(ate.USER_CODE)


WHERE al.TERMINAL_ID IS NULL
ORDER BY ate.USER_CODE
