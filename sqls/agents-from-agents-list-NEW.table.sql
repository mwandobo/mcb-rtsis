SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                                            AS reportingDate,
       al.AGENT_NAME                                                                                AS agentName,
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
       al.REGION                                                                                    AS region,
       al.DISTRICT                                                                                  AS district,
       null                                                                                         AS ward,
       null                                                                                         AS street,
       null                                                                                         AS houseNumber,
       null                                                                                         AS postalCode,
       'TANZANIA, UNITED REPUBLIC OF'                                                               AS country,
       al.GPS                                                                                       AS gpsCoordinates,
       al.TIN                                                                                       AS agentTaxIdentificationNumber,
       CASE
           -- 1️⃣ Comma exists
           WHEN LOCATE(',', al.BUSINESS_LICENCE_ISSUER_AND_DATE) > 0 THEN
               CASE
                   -- 1a. If there's a space before the comma, use first word (up to first space)
                   WHEN LOCATE(' ', SUBSTR(al.BUSINESS_LICENCE_ISSUER_AND_DATE, 1, LOCATE(',', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1)) > 0 THEN
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
           END AS businessLicense
FROM AGENTS_LIST al
         LEFT JOIN BANKEMPLOYEE be ON be.STAFF_NO = al.TERMINAL_ID