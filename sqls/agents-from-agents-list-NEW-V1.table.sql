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
       al.BUSINESS_FORM                                                                             AS businessForm,
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
       COALESCE(al.REGION, 'N/A')                                                                   AS region,
       COALESCE(al.DISTRICT, 'N/A')                                                                 AS district,
       'N/A'                                                                                        AS ward,
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
         RIGHT JOIN BANKEMPLOYEE be
                    ON RIGHT(TRIM(al.TERMINAL_ID), 8) = TRIM(be.STAFF_NO)
WHERE be.STAFF_NO IS NOT NULL
  AND be.STAFF_NO = TRIM(be.STAFF_NO)
  AND be.EMPL_STATUS = 1
  AND be.STAFF_NO NOT LIKE 'ATMUSER%'
  AND be.STAFF_NO NOT LIKE '993%'
  AND be.STAFF_NO NOT LIKE '999%'
  AND be.STAFF_NO NOT LIKE '900%'
  AND be.STAFF_NO NOT LIKE 'IAP%'
  AND be.STAFF_NO NOT LIKE 'MCB%'
  AND be.STAFF_NO NOT LIKE 'MIP%'
  AND be.STAFF_NO NOT LIKE 'MOB%'
  AND be.STAFF_NO NOT LIKE 'MWL%'
  AND be.STAFF_NO NOT LIKE 'OWP%'
  AND be.STAFF_NO NOT LIKE 'PI0%'
  AND be.STAFF_NO NOT LIKE 'POS%'
  AND be.STAFF_NO NOT LIKE 'STP%'
  AND be.STAFF_NO NOT LIKE 'TER%'
  AND be.STAFF_NO NOT LIKE 'EIC%'
  AND be.STAFF_NO NOT LIKE 'GEP%'
  AND be.STAFF_NO NOT LIKE 'EYU%'
  AND be.STAFF_NO NOT LIKE 'GLA%'
  AND be.STAFF_NO NOT LIKE 'SYS%'
  AND be.STAFF_NO NOT LIKE 'MLN%'
  AND be.STAFF_NO NOT LIKE 'PET%'
  AND be.STAFF_NO NOT LIKE 'VRT%';
