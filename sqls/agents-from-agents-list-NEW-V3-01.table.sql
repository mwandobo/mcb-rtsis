SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')           AS reportingDate,
       al.AGENT_NAME                                               AS agentName,
       al.TERMINAL_ID                                              AS TerminalID,
       al.AGENT_ID                                                 AS agentId,
       al.TILL_NUMBER                                              AS tillNumber,
       al.BUSINESS_FORM                                            AS businessForm,
       al.AGENT_PRINCIPAL                                          AS agentPrincipal,
       al.AGENT_PRINCIPAL_NAME                                     AS agentPrincipalName,
       al.GENDER                                                   AS gender,
       VARCHAR_FORMAT(be.TMSTAMP, 'DDMMYYYYHHMM')                  AS registrationDate,
       al.CLOSED_DATE                                              AS closedDate,
       al.CERT_INCORPORATION                                       AS certIncorporation,
       al.NATIONALITY                                              AS nationality,
       CASE WHEN al.IS_ACTIVE = 1 then 'Active' ELSE 'Dormant' END AS agentStatus,
       al.AGENT_STATUS                                             AS agentType,
       al.ACCOUNT_NUMBER                                           AS accountNumber,
       al.REGION                                                   AS region,
       COALESCE(region_lkp.BOT_REGION, '')                         AS region,
       al.DISTRICT                                                 AS district,
       COALESCE(district_lkp.BOT_DISTRICT, '')                     AS district,
       al.WARD                                                     AS ward,
       COALESCE(ward_lkp.BOT_WARD, '')                             AS ward,
       al.STREET                                                   AS street,
       al.HOUSE_NUMBER                                             AS houseNumber,
       al.POSTAL_CODE                                              AS postalCode,
       al.COUNTRY                                                  AS country,
       al.GPS_COORDINATES                                          AS gpsCoordinates,
       al.AGENT_TAX_IDENTIFICATION_NUMBER                          AS agentTaxIdentificationNumber,
       al.BUSINESS_LICENCE                                         AS businessLicense,
       al.IS_ACTIVE                                         AS IS_ACTIVE
FROM AGENTS_LIST_V3 al
         JOIN BANKEMPLOYEE be
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
                    FROM AGENTS_LIST_V3 al
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
                    FROM AGENTS_LIST_V3 al
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
                                       WHEN UPPER(TRIM(al.WARD)) = UPPER(TRIM(bl.WARD)) THEN 1

                                       -- 2️⃣ Starts-with match (safe)
                                       WHEN UPPER(TRIM(al.WARD)) LIKE UPPER(TRIM(bl.WARD)) || '%'
                                           AND LENGTH(TRIM(bl.WARD)) >= 4 THEN 2

                                       -- 3️⃣ No fallback
                                       ELSE 99
                                       END,
                                   LENGTH(TRIM(bl.WARD)) DESC
                               )   AS rn
                    FROM AGENTS_LIST_V3 al
                             JOIN BANK_LOCATION_LOOKUP_V2 bl
                                  ON (
                                      UPPER(TRIM(al.WARD)) = UPPER(TRIM(bl.WARD))
                                          OR (
                                          UPPER(TRIM(al.WARD)) LIKE UPPER(TRIM(bl.WARD)) || '%'
                                              AND LENGTH(TRIM(bl.WARD)) >= 4
                                          )
                                      )
                    WHERE TRIM(al.WARD) IS NOT NULL
                      AND TRIM(al.WARD) <> '') ward_lkp
                   ON ward_lkp.AGENT_ID = al.AGENT_ID
                       AND ward_lkp.rn = 1


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
;