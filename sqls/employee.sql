SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                                            AS reportingDate,
       c.FK_BRANCH_PORTFBRA                                                                         AS branchCode,
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
       )                                                                                            AS empName,
       CASE WHEN be.SEX = 'M' then 'Male' WHEN be.SEX = 'F' then 'female' ELSE 'Not Applicable' END AS gender,
       VARCHAR_FORMAT(c.DATE_OF_BIRTH, 'DDMMYYYYHHMM')                                              AS empDob,
       'NationalIdentityCard'                                                                       AS empIdentificationType,
       id.ID_NO                                                                                     AS empIdentificationNumber,
       COALESCE(position_data.DESCRIPTION, 'N/A')                                                   AS empPosition,
       CASE
           WHEN UPPER(COALESCE(position_data.DESCRIPTION, '')) LIKE '%SENIOR%' THEN 'Senior management'
           ELSE 'Non-Senior management'
           END                                                                                      AS empPositionCategory,
       'Permanent and pensionable'                                                                  AS empStatus,
       COALESCE(department_data.DESCRIPTION, 'N/A')                                                 AS empDepartment,
       VARCHAR_FORMAT(c.TMSTAMP, 'DDMMYYYYHHMM')                                                    AS appointmentDate,
       'TANZANIA, UNITED REPUBLIC OF'                                                               AS empNationality,
       VARCHAR_FORMAT(be.TMSTAMP, 'DDMMYYYYHHMM')                                                   AS lastPromotionDate,
       c.SALARY_AMN                                                                                 AS basicSalary,
       null                                                                                         AS benefitType,
       null                                                                                         AS benefitAmount
FROM BANKEMPLOYEE be
         LEFT JOIN (SELECT *
                    FROM (SELECT c.*,
                                 ROW_NUMBER() OVER (
                                     PARTITION BY
                                         UPPER(TRIM(c.FIRST_NAME)),
                                         UPPER(TRIM(c.SURNAME))
                                     ORDER BY c.CUST_ID
                                     ) AS rn
                          FROM CUSTOMER c) x
                    WHERE rn = 1) c
                   ON UPPER(TRIM(c.FIRST_NAME)) = UPPER(TRIM(be.FIRST_NAME))
                       AND UPPER(TRIM(c.SURNAME)) = UPPER(TRIM(be.LAST_NAME))
         LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
                                   id.fk_customercust_id = c.cust_id)
         LEFT JOIN generic_detail position_data ON (be.FKGH_WORKS_IN_POSI = position_data.fk_generic_headpar AND
                                                    be.FKGD_WORKS_IN_POSI = position_data.serial_num)
         LEFT JOIN generic_detail department_data ON (be.FKGH_HAS_AS_GRADE = department_data.fk_generic_headpar AND
                                                      be.FKGD_HAS_AS_GRADE = department_data.serial_num)
WHERE be.STAFF_NO IS NOT NULL
  AND be.STAFF_NO = TRIM(be.STAFF_NO)
  AND be.EMPL_STATUS = 1
  AND be.STAFF_NO LIKE 'EIC%'
  AND LENGTH(TRIM(id.ID_NO)) = 20
;
