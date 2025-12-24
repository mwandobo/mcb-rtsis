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
       CAST(be.STAFF_NO AS VARCHAR(50))                                                             AS agentId,
       be.STAFF_NO                                                                                  AS tillNumber,
       'Sole Proprietor'                                                                            AS businessForm,
       'bank'                                                                                       AS agentPrincipal,
       'Selcom'                                                                                     AS agentPrincipalName,
       CASE WHEN be.SEX = 'M' then 'Male' WHEN be.SEX = 'F' then 'female' ELSE 'Not Applicable' END AS gender,
       VARCHAR_FORMAT(COALESCE(be.TMSTAMP, CURRENT_DATE), 'DDMMYYYYHHMM')                           AS registrationDate,
       null                                                                                         AS closedDate,
       null                                                                                         AS certIncorporation,
       'TANZANIA, UNITED REPUBLIC OF'                                                               AS nationality,
       CASE
           WHEN be.EMPL_STATUS = '1' THEN 'Active'
           WHEN be.EMPL_STATUS = '0' THEN 'Inactive'
           ELSE 'Suspended'
           END                                                                                      AS agentStatus,
       'super agent'                                                                                AS agentType,
       null                                                                                         AS accountNumber,
       null                                                                                         AS region,
       null                                                                                         AS district,
       null                                                                                         AS ward,
       null                                                                                         AS street,
       null                                                                                         AS houseNumber,
       null                                                                                         AS postalCode,
       null                                                                                         AS country,
       null                                                                                         AS gpsCoordinates,
       null                                                                                         AS agentTaxIdentificationNumber,
       null                                                                                         AS businessLicense
FROM BANKEMPLOYEE be
WHERE STAFF_NO IS NOT NULL
  AND STAFF_NO = TRIM(STAFF_NO)
  AND EMPL_STATUS = 1
  AND STAFF_NO NOT LIKE 'ATMUSER%'
  AND STAFF_NO NOT LIKE '993%'
  AND STAFF_NO NOT LIKE '999%'
  AND STAFF_NO NOT LIKE '900%'
  AND STAFF_NO NOT LIKE 'IAP%'
  AND STAFF_NO NOT LIKE 'MCB%'
  AND STAFF_NO NOT LIKE 'MIP%'
  AND STAFF_NO NOT LIKE 'MOB%'
  AND STAFF_NO NOT LIKE 'MWL%'
  AND STAFF_NO NOT LIKE 'OWP%'
  AND STAFF_NO NOT LIKE 'PI0%'
  AND STAFF_NO NOT LIKE 'POS%'
  AND STAFF_NO NOT LIKE 'STP%'
  AND STAFF_NO NOT LIKE 'TER%'
  AND STAFF_NO NOT LIKE 'EIC%'
  AND STAFF_NO NOT LIKE 'GEP%'
  AND STAFF_NO NOT LIKE 'EYU%'
  AND STAFF_NO NOT LIKE 'GLA%'
  AND STAFF_NO NOT LIKE 'SYS%'
  AND STAFF_NO NOT LIKE 'MLN%'
  AND STAFF_NO NOT LIKE 'PET%'
  AND STAFF_NO NOT LIKE 'VRT%';
