SELECT CURRENT_TIMESTAMP                               AS reportingDate,
       c.SURNAME                                       AS companyName,
       TRIM(c.cust_id)                                 AS customerIdentificationNumber,
       c.date_of_birth                                 AS establishmentDate,
       'LimitedLiabilityCompanyPrivate'                AS legalForm,
       'NoNegativeStatus'                              AS negativeClientStatus,
       c.NO_OF_EMPLOYEES                               AS numberOfEmployees,
       c.NO_OF_EMPLOYEES                               AS numberOfEmployeesMale,
       c.NO_OF_EMPLOYEES                               AS numberOfEmployeesFemale,
       CASE UPPER(TRIM(id_country.description))
           WHEN 'TANZANIA'
               THEN 'TANZANIA, UNITED REPUBLIC OF' END AS registrationCountry,
       id.id_no                                        AS registrationNumber,
       wdc.TAX_REGISTRATION_NO                         AS taxIdentificationNumber,
       c.SURNAME                                       AS tradeName,
       NULL                                            AS parentName,
       NULL                                            AS parentIncorporationNumber,
       NULL                                            AS groupId,
       'Other financial Corporations'                  AS sectorSnaClassification,
       wdc.NAME_STANDARD                               AS fullName,
       CASE
           WHEN c.sex = 'M' THEN 'Male'
           WHEN c.sex = 'F' THEN 'Female'
           ELSE 'Not Applicable'
           END                                         AS gender,
       wdc.TELEPHONE                                   AS cellPhone,

       'Director'                                      AS relationType,
       wdc.ID_NO                                       AS nationalId,
       NULL                                            AS appointmentDate,
       NULL                                            AS terminationDate,
       NULL                                            AS rateValueOfSharesOwned,
       NULL                                            AS amountValueOfSharesOwned,

       NULL                                            AS street,
       CASE UPPER(TRIM(id_country.description))
           WHEN 'TANZANIA'
               THEN 'TANZANIA, UNITED REPUBLIC OF' END AS country,
       c_address.CITY                                  AS region,
       c_address.REGION                                AS district,
       c_address.ADDRESS_1                             AS ward,
       NULL                                            AS houseNumber,
       NULL                                            AS postalCode,
       NULL                                            AS poBox,
       c_address.ZIP_CODE                              AS zipCode,
       NULL                                            AS primaryPostalCode,
       c_address.CITY                                  AS primaryRegion,
       c_address.REGION                                AS primaryDistrict,
       c_address.ADDRESS_1                             AS primaryWard,
       NULL                                            AS primaryStreet,
       NULL                                            AS primaryHouseNumber,
       NULL                                            AS secondaryStreet,
       NULL                                            AS secondaryHouseNumber,
       NULL                                            AS secondaryPostalCode,
       NULL                                            AS secondaryRegion,
       NULL                                            AS secondaryDistrict,
       NULL                                            AS secondaryCountry,
       NULL                                            AS secondaryTextAddress,
       wdc.TELEPHONE                                   AS mobileNumber,
       NULL                                            AS alternativeMobileNumber,
       NULL                                            AS fixedLineNumber,
       NULL                                            AS faxNumber,
       NULL                                            AS emailAddress,
       NULL                                            AS socialMedia,
       c.SURNAME                                       AS entityName,
       'LimitedLiabilityCompanyPrivate'                AS entityType,
       NULL                                            AS certificateIncorporation,
       NULL                                            AS entityRegion,
       NULL                                            AS entityDistrict,
       NULL                                            AS entityWard,
       NULL                                            AS entityStreet,
       NULL                                            AS entityHouseNumber,
       NULL                                            AS entityPostalCode,
       NULL                                            AS groupParentCode,
       NULL                                            AS shareOwnedPercentage,
       NULL                                            AS shareOwnedAmount
FROM customer c
         INNER JOIN (SELECT CUST_ID,
                            MOBILE_TEL,
                            TAX_REGISTRATION_NO,
                            ID_NO,
                            NATIONAL_DESCRIPTION,
                            TELEPHONE,
                            NAME_STANDARD,
                            ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUST_ID) AS rn -- Keeps one row per customer
                     FROM W_DIM_CUSTOMER
                     WHERE CUST_TYPE_IND = 'Corporate') wdc ON wdc.CUST_ID = c.CUST_ID AND wdc.rn = 1
         LEFT JOIN cust_address c_address
                   ON c_address.fk_customercust_id = c.cust_id
                       AND c_address.communication_addr = '1'
                       AND c_address.entry_status = '1'
         LEFT JOIN other_id id
                   ON id.fk_customercust_id = c.cust_id
                       AND (CASE WHEN id.serial_no IS NULL THEN '1' ELSE id.main_flag END) = '1'

         LEFT JOIN generic_detail id_country
                   ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                       AND id.fkgd_has_been_issu = id_country.serial_num
WHERE CUST_TYPE = '2';
