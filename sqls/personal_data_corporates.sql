select CURRENT_TIMESTAMP                               AS reportingDate,
       cust.SURNAME                                    AS companyName,
       TRIM(cust.cust_id)                              AS customerIdentificationNumber,
       cust.date_of_birth                              AS establishmentDate,
       'LimitedLiabilityCompanyPrivate'                AS legalForm,
       'NoNegativeStatus'                              AS negativeClientStatus,
       cust.NO_OF_EMPLOYEES                            AS numberOfEmployees,
       cust.NO_OF_EMPLOYEES                            AS numberOfEmployeesMale,
       cust.NO_OF_EMPLOYEES                            AS numberOfEmployeesFemale,
       CASE UPPER(TRIM(id_country.description))
           WHEN 'TANZANIA'
               THEN 'TANZANIA, UNITED REPUBLIC OF' END AS registrationCountry,
       id.id_no                                        AS registrationNumber,
       wdc.TAX_REGISTRATION_NO                         AS taxIdentificationNumber,
       cust.SURNAME                                    AS tradeName,
       NULL                                            AS parentName,
       NULL                                            AS parentIncorporationNumber,
       NULL                                            AS groupId,
       'Other financial Corporations'                  AS sectorSnaClassification,
       wdc.NAME_STANDARD                               AS fullName,
       CASE
           WHEN cust.sex = 'M' THEN 'Male'
           WHEN cust.sex = 'F' THEN 'Female'
           ELSE 'Not Applicable'
           END                                         AS gender,
       wdc.TELEPHONE                                   AS cellPhone,

       'Director'                                      AS relationType,
       wdc.ID_NO                                       AS nationalId,
       CASE UPPER(TRIM(id_country.description))
           WHEN 'TANZANIA'
               THEN 'TANZANIA, UNITED REPUBLIC OF' END AS nationality,
       'N/A'                                           AS appointmentDate,
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
       cust.SURNAME                                    AS entityName,
       'LimitedLiabilityCompanyPrivate'                AS entityType,
       NULL                                            AS certificateIncorporation,
       c_address.CITY                                  AS entityRegion,
       c_address.REGION                                AS entityDistrict,
       NULL                                            AS entityWard,
       NULL                                            AS entityStreet,
       NULL                                            AS entityHouseNumber,
       NULL                                            AS entityPostalCode,
       NULL                                            AS groupParentCode,
       NULL                                            AS shareOwnedPercentage,
       NULL                                            AS shareOwnedAmount
from CUSTOMER cust
         INNER JOIN (SELECT CUST_ID,
                            MOBILE_TEL,
                            TAX_REGISTRATION_NO,
                            ID_NO,
                            NATIONAL_DESCRIPTION,
                            TELEPHONE,
                            NAME_STANDARD,
                            ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUST_ID) AS rn -- Keeps one row per customer
                     FROM W_DIM_CUSTOMER
                     WHERE CUST_TYPE_IND = 'Corporate') wdc ON wdc.CUST_ID = cust.CUST_ID AND wdc.rn = 1
         LEFT JOIN cust_address c_address
                   ON c_address.fk_customercust_id = cust.cust_id
                       AND c_address.communication_addr = '1'
                       AND c_address.entry_status = '1'
         LEFT JOIN other_id id
                   ON id.fk_customercust_id = cust.cust_id
                       AND (CASE WHEN id.serial_no IS NULL THEN '1' ELSE id.main_flag END) = '1'

         LEFT JOIN generic_detail id_country
                   ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                       AND id.fkgd_has_been_issu = id_country.serial_num
         JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = cust.CUST_ID AND pa.PRODUCT_ID = 31704;
