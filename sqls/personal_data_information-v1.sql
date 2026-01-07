WITH ccategory AS (SELECT customer_category.*, fk_generic_headpar, gd.description
                   FROM customer_category,
                        generic_detail gd
                   WHERE fk_generic_detafk = gd.fk_generic_headpar
                     AND fk_generic_detaser = gd.serial_num
                     AND gd.fk_generic_headpar = gd.fk_generic_headpar),
     family AS (SELECT description, fk_customercust_id
                FROM ccategory
                WHERE fk_categorycategor = 'FAMILY'
                  AND fk_generic_headpar = 'FALST'),
     employmentStatusData AS (SELECT description, fk_customercust_id
                              FROM ccategory
                              WHERE fk_categorycategor = 'PROFLEVL'
                                AND fk_generic_headpar = 'PRFST')
SELECT CURRENT_TIMESTAMP                                                                          AS reportingDate,
       TRIM(c.cust_id)                                                                            AS customerIdentificationNumber,
       c.first_name                                                                               AS firstName,
       c.middle_name                                                                              AS middleNames,
       c.SURNAME                                                                                  AS otherNames,
       TRIM(
               CASE
                   WHEN c.cust_type = '1' THEN
                       TRIM(NVL(c.first_name, ' ')) || ' ' ||
                       TRIM(NVL(c.middle_name, ' ')) || ' ' ||
                       TRIM(NVL(c.surname, ' '))
                   WHEN c.cust_type = '2' THEN
                       TRIM(c.surname)
                   ELSE ''
                   END
       )                                                                                          AS "fullNames",
       c.surname                                                                                  AS presentSurname,
       c.surname                                                                                  AS birthSurname,
       CASE WHEN c.SEX = 'M' then 'Male' WHEN c.SEX = 'F' then 'female' ELSE 'Not Applicable' END AS gender,
       CASE UPPER(TRIM(family.DESCRIPTION))
           WHEN 'MARRIED' THEN 'Married'
           WHEN 'SINGLE' THEN 'Single'
           WHEN 'DIVORCED' THEN 'Divorced'
           WHEN 'WIDOWED' THEN 'Widowed'
           ELSE 'Single'
           END                                                                                    AS maritalStatus,
       NULL                                                                                       AS numberSpouse,
       gd_natio.description                                                                       AS nationality,
       gd_citiz.description                                                                       AS citizenship,
       CASE WHEN c.non_resident = '0' THEN 'Resident' ELSE 'Non-Resident' END                     AS residency,
       gd_proff.description                                                                       AS profession,
       'Households'                                                                               AS sectorSnaClassification,
       'No Fate'                                                                                  AS fateStatus,
       'N/A'                                                                                      AS socialStatus,
       CASE UPPER(TRIM(employmentStatusData.description))
           WHEN 'EMPLOYED' THEN 'Employed'
           WHEN 'SALARIED' THEN 'Employed'
           WHEN 'CUSTOMER SERVICE' THEN 'Self-employed'
           ELSE 'Unemployed'
           END                                                                                    AS employmentStatus,
       c.salary_amn                                                                               AS monthlyIncome,
       c.num_of_children + c.children_above18                                                     AS numberDependants,
       gd_edulevel.description                                                                    AS educationLevel,
       NULL                                                                                       AS averageMonthlyExpenditure,
       NULL                                                                                       AS monthlyExpenses,
       c.blacklisted_ind                                                                          AS negativeClientStatus,
       c.spouse_name                                                                              AS spousesFullName,
       NULL                                                                                       AS spouseIdentificationType,
       NULL                                                                                       AS spouseIdentificationNumber,
       NULL                                                                                       AS maidenName,
       NULL                                                                                       AS educationLevel,
       c.date_of_birth                                                                            AS birthDate,
       id_country.description                                                                     AS birthCountry,
       NULL                                                                                       AS birthPostalCode,
       NULL                                                                                       AS birthHouseNumber,
       NULL                                                                                       AS birthRegion,
       NULL                                                                                       AS birthDistrict,
       NULL                                                                                       AS birthWard,
       NULL                                                                                       AS birthStreet,
       idt.description                                                                            AS identificationType,
       id.id_no                                                                                   AS identificationNumber,
       id.issue_date                                                                              AS issuanceDate,
       id.expiry_date                                                                             AS expirationDate,
       NULL                                                                                       AS issuancePlace,
       'NIDA'                                                                                     AS issuingAuthority,

       NULL                                                                                       AS businessName,
       NULL                                                                                       AS establishmentDate,
       NULL                                                                                       AS businessRegistrationNumber,
       NULL                                                                                       AS businessRegistrationDate,
       NULL                                                                                       AS businessLicenseNumber,
       NULL                                                                                       AS taxIdentificationNumber,

       NULL                                                                                       AS employerName,
       NULL                                                                                       AS employerRegion,
       NULL                                                                                       AS employerDistrict,
       NULL                                                                                       AS employerWard,
       NULL                                                                                       AS employerStreet,
       NULL                                                                                       AS employerHouseNumber,
       NULL                                                                                       AS employerPostalCode,
       NULL                                                                                       AS businessNature,

       c.mobile_tel                                                                               AS mobileNumber,
       c.mobile_tel2                                                                              AS alternativeMobileNumber,
       c.telephone_1                                                                              AS fixedLineNumber,
       c_address.fax_no                                                                           AS faxNumber,
       c.e_mail                                                                                   AS emailAddress,
       c.internet_address                                                                         AS socialMedia,

       c_address.address_1 || ' ' || c_address.address_2                                          AS mainAddress,
       NULL                                                                                       AS street,
       NULL                                                                                       AS houseNumber,
       c_address.zip_code                                                                         AS postalCode,
       c_address.region                                                                           AS region,
       NULL                                                                                       AS district,
       NULL                                                                                       AS ward,
       c_country.description                                                                      AS country,

       NULL                                                                                       AS sstreet,
       NULL                                                                                       AS shouseNumber,
       NULL                                                                                       AS spostalCode,
       NULL                                                                                       AS sregion,
       NULL                                                                                       AS sdistrict,
       NULL                                                                                       AS sward,
       NULL                                                                                       AS scountry

FROM customer c
         LEFT JOIN cust_address c_address
                   ON c_address.fk_customercust_id = c.cust_id
                       AND c_address.communication_addr = '1'
                       AND c_address.entry_status = '1'
         LEFT JOIN generic_detail c_country
                   ON c_address.fkgd_has_country = c_country.serial_num
                       AND c_address.fkgh_has_country = c_country.fk_generic_headpar

         LEFT JOIN other_id id
                   ON id.fk_customercust_id = c.cust_id
                       AND (CASE WHEN id.serial_no IS NULL THEN '1' ELSE id.main_flag END = '1')

         LEFT JOIN generic_detail id_country
                   ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                       AND id.fkgd_has_been_issu = id_country.serial_num

         LEFT JOIN generic_detail idt
                   ON idt.fk_generic_headpar = id.fkgh_has_type
                       AND idt.serial_num = id.fkgd_has_type
         LEFT JOIN other_afm afm
                   ON afm.fk_customercust_id = c.cust_id
                       AND (CASE
                                WHEN afm.serial_no IS NULL THEN '1'
                                ELSE CASE
                                         WHEN c.no_afm = '1' THEN CAST(afm.serial_no AS VARCHAR(2))
                                         ELSE afm.main_flag END
                                END = '1')
         LEFT JOIN customer_category cc_natio
                   ON cc_natio.fk_customercust_id = c.cust_id
                       AND cc_natio.fk_categorycategor = 'NATIONAL'
                       AND cc_natio.fk_generic_detafk = 'NATIO'
         LEFT JOIN generic_detail gd_natio
                   ON cc_natio.fk_generic_detafk = gd_natio.fk_generic_headpar
                       AND cc_natio.fk_generic_detaser = gd_natio.serial_num
         LEFT JOIN customer_category cc_citiz
                   ON cc_citiz.fk_customercust_id = c.cust_id
                       AND cc_citiz.fk_categorycategor = 'CITIZEN'
                       AND cc_citiz.fk_generic_detafk = 'CITIZ'
         LEFT JOIN generic_detail gd_citiz
                   ON cc_citiz.fk_generic_detafk = gd_citiz.fk_generic_headpar
                       AND cc_citiz.fk_generic_detaser = gd_citiz.serial_num
         LEFT JOIN customer_category cc_proff
                   ON cc_proff.fk_customercust_id = c.cust_id
                       AND cc_proff.fk_categorycategor = 'PROFES'
                       AND cc_proff.fk_generic_detafk = 'PROFF'
         LEFT JOIN generic_detail gd_proff
                   ON cc_proff.fk_generic_detafk = gd_proff.fk_generic_headpar
                       AND cc_proff.fk_generic_detaser = gd_proff.serial_num
         LEFT JOIN customer_category cc_ccode
                   ON cc_ccode.fk_customercust_id = c.cust_id
                       AND cc_ccode.fk_categorycategor = 'ACTIVITY'
                       AND cc_ccode.fk_generic_detafk = 'CCODE'

         LEFT JOIN customer_category cc_profcat
                   ON cc_profcat.fk_customercust_id = c.cust_id
                       AND cc_profcat.fk_categorycategor = 'PROFCAT'
                       AND cc_profcat.fk_generic_detafk = 'EMPTP'

         LEFT JOIN customer_category cc_edulevel
                   ON cc_edulevel.fk_customercust_id = c.cust_id
                       AND cc_edulevel.fk_categorycategor = 'EDULEVEL'
                       AND cc_edulevel.fk_generic_detafk = 'EDULV'
         LEFT JOIN generic_detail gd_edulevel
                   ON cc_edulevel.fk_generic_detafk = gd_edulevel.fk_generic_headpar
                       AND cc_edulevel.fk_generic_detaser = gd_edulevel.serial_num


         LEFT JOIN family ON (family.fk_customercust_id = c.cust_id)
         LEFT JOIN employmentStatusData ON (employmentStatusData.fk_customercust_id = c.cust_id)
;
