SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       al.AGENT_NAME                                     AS agentName,
       al.AGENT_ID                                       AS agentId,
       al.TILL_NUMBER                                    AS tillNumber,
       COALESCE(al.BUSINESS_FORM, 'Sole Proprietor')     AS businessForm,
       al.AGENT_PRINCIPAL                                AS agentPrincipal,
       al.AGENT_PRINCIPAL_NAME                           AS agentPrincipalName,
       al.GENDER                                         AS gender,
       al.REGISTRATION_DATE                              AS registrationDate,
       al.CLOSED_DATE                                    AS closedDate,
       al.CERT_INCORPORATION                             AS certIncorporation,
       al.NATIONALITY                                    AS nationality,
       CASE
           WHEN be.EMPL_STATUS = '1' THEN 'Active'
           WHEN be.EMPL_STATUS = '0' THEN 'Inactive'
           ELSE 'Suspended'
           END                                           AS agentStatus,
       al.AGENT_TYPE                                     AS agentType,
       al.ACCOUNT_NUMBER                                 AS accountNumber,
       al.REGION                                         AS region,
       al.DISTRICT                                       AS district,
       al.WARD                                           AS ward,
       al.STREET                                         AS street,
       al.HOUSE_NUMBER                                   AS houseNumber,
       al.POSTAL_CODE                                    AS postalCode,
       al.COUNTRY                                        AS country,
       COALESCE(al.GPS_COORDINATES, '-6.7725°,38.9769°') AS gpsCoordinates,
       al.AGENT_TAX_IDENTIFICATION_NUMBER                AS agentTaxIdentificationNumber,
       al.BUSINESS_LICENCE                               AS businessLicense
FROM AGENTS_LIST_V4 al
         JOIN BANKEMPLOYEE be
              ON
                  CASE
                      WHEN LENGTH(REPLACE(al.TERMINAL_ID, ' ', '')) > 8
                          THEN RIGHT(REPLACE(al.TERMINAL_ID, ' ', ''), 8)
                      ELSE REPLACE(al.TERMINAL_ID, ' ', '')
                      END
                      =
                  CASE
                      WHEN LENGTH(REPLACE(be.STAFF_NO, ' ', '')) > 8
                          THEN RIGHT(REPLACE(be.STAFF_NO, ' ', ''), 8)
                      ELSE REPLACE(be.STAFF_NO, ' ', '')
                      END