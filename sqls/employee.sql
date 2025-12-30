SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                                            AS reportingDate,
       201                                                                                          AS branchCode,
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
       'bank'                                                                                       AS empPosition,
       'Selcom'                                                                                     AS empPositionCategory,
       VARCHAR_FORMAT(COALESCE(be.TMSTAMP, CURRENT_DATE), 'DDMMYYYYHHMM')                           AS empStatus,
       null                                                                                         AS empDepartment,
       null                                                                                         AS appointmentDate,
       'TANZANIA, UNITED REPUBLIC OF'                                                               AS empNationality,
       null                                                                                         AS lastPromotionDate,
       null                                                                                         AS basicSalary
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
WHERE STAFF_NO IS NOT NULL
  AND STAFF_NO = TRIM(STAFF_NO)
  AND EMPL_STATUS = 1
  AND STAFF_NO LIKE 'EIC%';
