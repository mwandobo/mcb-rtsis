WITH w_address AS (
    SELECT *
    FROM   cust_address c
    WHERE  (c.fk_customercust_id, c.serial_num) IN (
        SELECT fk_customercust_id,
               MIN(serial_num)
        FROM   cust_address
        WHERE  address_type = '4'
          AND  entry_status = '1'
        GROUP BY fk_customercust_id
    )
)
SELECT
    CURRENT_TIMESTAMP AS reportingDate,

    c.cust_id AS customerIdentificationNumber,

    c.first_name AS firstName,
    c.middle_name AS middleNames,
    NULL AS otherNames,
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
    ) AS "fullNames",

    c.surname AS presentSurname,
    c.mother_surname AS birthSurname,
    c.sex AS gender,

    NULL AS maritalStatus,

    NULL AS numberSpouse,
    c.spouse_name AS spousesFullName,

    gd_natio.description AS nationality,
    gd_citiz.description AS citizenship,
    CASE WHEN c.non_resident = '0' THEN 'Resident' ELSE 'Non-Resident' END AS residency,

    gd_proff.description AS profession,
    gd_ccode.description AS sectorSnaClassification,

    NULL AS fateStatus,
    NULL AS socialStatus,

    gd_profcat.description AS employmentStatus,

    c.salary_amn AS monthlyIncome,

    c.num_of_children + c.children_above18 AS numberDependants,

    gd_edulevel.description AS educationLevel,

    NULL AS averageMonthlyExpenditure,
    NULL AS monthlyExpenses,

    c.blacklisted_ind AS negativeClientStatus,

    c.spouse_name AS spousesFullName,
    idt.description AS spouseIdentificationType,
    NULL AS spouseIdentificationNumber,

    c.mother_surname AS maidenName,

    c.date_of_birth AS birthDate,
    id_country.description AS birthCountry,
    NULL AS birthPostalCode,
    NULL AS birthHouseNumber,
    NULL AS birthRegion,
    NULL AS birthDistrict,
    NULL AS birthWard,
    NULL AS birthStreet,

    idt.description AS identificationType,
    id.id_no AS identificationNumber,
    id.issue_date AS issuanceDate,
    id.expiry_date AS expirationDate,
    NULL AS issuancePlace,
    NULL AS issuingAuthority,

    c.short_name AS businessName,
    NULL AS establishmentDate,
    NULL AS businessRegistrationNumber,
    NULL AS businessRegistrationDate,
    NULL AS businessLicenseNumber,
    afm.afm_no AS taxIdentificationNumber,

    c.employer AS employerName,
    NULL AS employerRegion,
    NULL AS employerDistrict,
    NULL AS employerWard,
    c.employer_address AS employerStreet,
    NULL AS employerHouseNumber,
    NULL AS employerPostalCode,
    gd_ccode.description AS businessNature,

    c.mobile_tel AS mobileNumber,
    c.mobile_tel2 AS alternativeMobileNumber,
    c.telephone_1 AS fixedLineNumber,
    c_address.fax_no AS faxNumber,
    c.e_mail AS emailAddress,
    c.internet_address AS socialMedia,

    c_address.address_1 || ' ' || c_address.address_2 AS mainAddress,
    c_address.address_1 AS street,
    NULL AS houseNumber,
    c_address.zip_code AS postalCode,
    c_address.region AS region,
    NULL AS district,
    NULL AS ward,
    c_country.description AS country,

    w_address.address_1 AS street,
    NULL AS houseNumber,
    w_address.zip_code AS postalCode,
    w_address.region AS region,
    NULL AS district,
    NULL AS ward,
    w_country.description AS country

FROM customer c
LEFT JOIN cust_address c_address
    ON c_address.fk_customercust_id = c.cust_id
   AND c_address.communication_addr = '1'
   AND c_address.entry_status = '1'
LEFT JOIN w_address
    ON w_address.fk_customercust_id = c.cust_id
LEFT JOIN generic_detail c_country
    ON c_address.fkgd_has_country = c_country.serial_num
   AND c_address.fkgh_has_country = c_country.fk_generic_headpar
LEFT JOIN generic_detail w_country
    ON w_address.fkgd_has_country = w_country.serial_num
   AND w_address.fkgh_has_country = w_country.fk_generic_headpar
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
   AND (CASE WHEN afm.serial_no IS NULL THEN '1'
             ELSE CASE WHEN c.no_afm = '1' THEN CAST(afm.serial_no AS VARCHAR(2))
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
LEFT JOIN generic_detail gd_ccode
    ON cc_ccode.fk_generic_detafk = gd_ccode.fk_generic_headpar
   AND cc_ccode.fk_generic_detaser = gd_ccode.serial_num
LEFT JOIN customer_category cc_profcat
    ON cc_profcat.fk_customercust_id = c.cust_id
   AND cc_profcat.fk_categorycategor = 'PROFCAT'
   AND cc_profcat.fk_generic_detafk = 'EMPTP'
LEFT JOIN generic_detail gd_profcat
    ON cc_profcat.fk_generic_detafk = gd_profcat.fk_generic_headpar
   AND cc_profcat.fk_generic_detaser = gd_profcat.serial_num
LEFT JOIN customer_category cc_edulevel
    ON cc_edulevel.fk_customercust_id = c.cust_id
   AND cc_edulevel.fk_categorycategor = 'EDULEVEL'
   AND cc_edulevel.fk_generic_detafk = 'EDULV'
LEFT JOIN generic_detail gd_edulevel
    ON cc_edulevel.fk_generic_detafk = gd_edulevel.fk_generic_headpar
   AND cc_edulevel.fk_generic_detaser = gd_edulevel.serial_num;
