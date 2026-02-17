SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       al.AGENT_NAME                                     AS agentName,
       al.TERMINAL_ID                                    AS terminalID,
       al.AGENT_ID                                       AS agentId,
       al.TILL_NUMBER                                    AS tillNumber,
       al.BUSINESS_FORM                                  AS businessForm,
       al.AGENT_PRINCIPAL                                AS agentPrincipal,
       al.AGENT_PRINCIPAL_NAME                           AS agentPrincipalName,
       al.GENDER                                         AS gender,
       al.REGISTRATION_DATE                              AS registrationDate,
       al.CLOSED_DATE                                    AS closedDate,
       al.CERT_INCORPORATION                             AS certIncorporation,
       al.NATIONALITY                                    AS nationality,
       al.AGENT_STATUS                                   AS agentStatus,
       al.AGENT_TYPE                                     AS agentType,
       al.ACCOUNT_NUMBER                                 AS accountNumber,
       al.REGION                                         AS region,
       al.DISTRICT                                       AS district,
       al.WARD                                           AS ward,
       al.STREET                                         AS street,
       al.HOUSE_NUMBER                                   AS houseNumber,
       al.POSTAL_CODE                                    AS postalCode,
       al.COUNTRY                                        AS country,
       al.GPS_COORDINATES                                AS gpsCoordinates,
       al.AGENT_TAX_IDENTIFICATION_NUMBER                AS agentTaxIdentificationNumber,
       al.BUSINESS_LICENCE                               AS businessLicense
FROM AGENTS_LIST_V3 al
WHERE al.AGENT_TAX_IDENTIFICATION_NUMBER IS NOT NULL
  AND TRIM(al.AGENT_TAX_IDENTIFICATION_NUMBER) <> ''
  AND al.BUSINESS_LICENCE IS NOT NULL
  AND TRIM(al.BUSINESS_LICENCE) <> '';