WITH corporate_customers AS
         (SELECT CUST_ID,
                 ID_NO
          FROM (SELECT CUST_ID,
                       ID_NO,
                       ROW_NUMBER() OVER
                           (
                           PARTITION BY CUST_ID
                           ORDER BY CUST_ID
                           ) AS rn
                FROM W_DIM_CUSTOMER
                WHERE CUST_TYPE_IND = 'Corporate') x
          WHERE rn = 1),

     corporate_agreements AS
         (SELECT DISTINCT a.AGR_SN,
                          ca.FK_CUSTOMERCUST_ID AS corporate_cust_id
          FROM AGREEMENT a
                   JOIN CUST_ADDRESS ca
                        ON a.FK_CUST_ADDRESSFK = ca.FK_CUSTOMERCUST_ID
                            AND a.FK_CUST_ADDRESSSER = ca.SERIAL_NUM

                   JOIN corporate_customers cc
                        ON cc.CUST_ID = ca.FK_CUSTOMERCUST_ID

          WHERE EXISTS
                    (SELECT 1
                     FROM PROFITS_ACCOUNT pa
                     WHERE pa.CUST_ID = ca.FK_CUSTOMERCUST_ID
                       AND pa.PRODUCT_ID = 31704))

SELECT CURRENT_TIMESTAMP                               AS reportingDate,
       corp.SURNAME                                    AS companyName,
       ca.corporate_cust_id                            AS customerIdentificationNumber,
       corp.DATE_OF_BIRTH                              AS establishedDate,
       'LimitedLiabilityCompanyPrivate'                AS legalForm,
       'NoNegativeStatus'                              AS negativeClientStatus,
       CASE UPPER(TRIM(id_country.description))
           WHEN 'TANZANIA'
               THEN 'TANZANIA, UNITED REPUBLIC OF' END AS registrationCountry,
       id.id_no                                        AS registrationNumber,
       cc.ID_NO                                        AS taxIdentificationNumber,
       corp.SURNAME                                    AS tradeName,
       NULL                                            AS parentName,
       NULL                                            AS parentIncorporationNumber,
       NULL                                            AS groupId,
       'Other financial Corporations'                  AS sectorSnaClassification,
       '[' ||
       RTRIM(
               CAST(
                       XMLSERIALIZE(
                               XMLAGG(
                                       XMLTEXT(
                                               '{' ||
                                               '"fullName":"' || REPLACE(COALESCE(TRIM(rel.FIRST_NAME) || ' ' ||
                                                                                  COALESCE(TRIM(rel.MIDDLE_NAME) || ' ', '') ||
                                                                                  TRIM(rel.SURNAME), ''), '"', '\"') ||
                                               '",' ||
                                               '"gender":' || CASE TRIM(rel.SEX)
                                                                  WHEN 'M' THEN '"Male"'
                                                                  WHEN 'F' THEN '"Female"'
                                                                  ELSE 'null' END || ',' ||
                                               '"relationType":"Director",' ||
                                               '"nationality":' || CASE UPPER(TRIM(id_country.description))
                                                                       WHEN 'TANZANIA'
                                                                           THEN '"TANZANIA, UNITED REPUBLIC OF"'
                                                                       ELSE 'null' END || ',' ||
                                               '"appointmentDate":"N/A",' ||
                                               '"terminationDate":null,' ||
                                               '"street":null,' ||
                                               '"country":' || CASE UPPER(TRIM(id_country.description))
                                                                   WHEN 'TANZANIA' THEN '"TANZANIA, UNITED REPUBLIC OF"'
                                                                   ELSE 'null' END || ',' ||
                                               '"region":' || CASE
                                                                  WHEN TRIM(c_address.CITY) IS NOT NULL
                                                                      THEN '"' || REPLACE(TRIM(c_address.CITY), '"', '\"') || '"'
                                                                  ELSE 'null' END || ',' ||
                                               '"district":' || CASE
                                                                    WHEN TRIM(c_address.REGION) IS NOT NULL
                                                                        THEN '"' || REPLACE(TRIM(c_address.REGION), '"', '\"') || '"'
                                                                    ELSE 'null' END || ',' ||
                                               '"ward":' || CASE
                                                                WHEN TRIM(c_address.ADDRESS_1) IS NOT NULL
                                                                    THEN '"' || REPLACE(TRIM(c_address.ADDRESS_1), '"', '\"') || '"'
                                                                ELSE 'null' END || ',' ||
                                               '"zipCode":' || CASE
                                                                   WHEN TRIM(c_address.ZIP_CODE) IS NOT NULL
                                                                       THEN '"' || REPLACE(TRIM(c_address.ZIP_CODE), '"', '\"') || '"'
                                                                   ELSE 'null' END || ',' ||
                                               '"primaryRegion":' || CASE
                                                                         WHEN TRIM(c_address.CITY) IS NOT NULL
                                                                             THEN '"' || REPLACE(TRIM(c_address.CITY), '"', '\"') || '"'
                                                                         ELSE 'null' END || ',' ||
                                               '"primaryDistrict":' || CASE
                                                                           WHEN TRIM(c_address.REGION) IS NOT NULL
                                                                               THEN '"' || REPLACE(TRIM(c_address.REGION), '"', '\"') || '"'
                                                                           ELSE 'null' END || ',' ||
                                               '"primaryWard":' || CASE
                                                                       WHEN TRIM(c_address.ADDRESS_1) IS NOT NULL
                                                                           THEN '"' || REPLACE(TRIM(c_address.ADDRESS_1), '"', '\"') || '"'
                                                                       ELSE 'null' END ||
                                               '},'
                                       )
                               ) AS CLOB
                       ) AS VARCHAR(32000)
               ), ','
       ) || ']'                                        AS related_customers,

       'N/A'                                           AS entityName,
       NULL                                            AS certificateIncorporation,
       'N/A'                                           AS entityRegion,
       'N/A'                                           AS entityDistrict,
       'N/A'                                           AS entityWard,
       'N/A'                                           AS entityStreet,
       'N/A'                                           AS entityHouseNumber,
       'N/A'                                           AS entityPostalCode,
       'N/A'                                           AS groupParentCode,
       NULL                                            AS shareOwnedPercentage,
       NULL                                            AS shareOwnedAmount

FROM corporate_agreements ca

         JOIN PROFITS.CUSTOMER corp
              ON corp.CUST_ID = ca.corporate_cust_id

         JOIN corporate_customers cc
              ON cc.CUST_ID = corp.CUST_ID

         JOIN AGREEMENT a2
              ON a2.AGR_SN = ca.AGR_SN

         JOIN CUST_ADDRESS ca2
              ON a2.FK_CUST_ADDRESSFK = ca2.FK_CUSTOMERCUST_ID
                  AND a2.FK_CUST_ADDRESSSER = ca2.SERIAL_NUM

         JOIN CUSTOMER rel
              ON rel.CUST_ID = ca2.FK_CUSTOMERCUST_ID

         LEFT JOIN cust_address c_address
                   ON c_address.fk_customercust_id = corp.cust_id
                       AND c_address.communication_addr = '1'
                       AND c_address.entry_status = '1'

         LEFT JOIN other_id id
                   ON id.fk_customercust_id = corp.cust_id
                       AND (CASE
                                WHEN id.serial_no IS NULL
                                    THEN '1'
                                ELSE id.main_flag
                           END) = '1'

         LEFT JOIN generic_detail id_country
                   ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                       AND id.fkgd_has_been_issu = id_country.serial_num

WHERE ca2.FK_CUSTOMERCUST_ID <> ca.corporate_cust_id
  AND rel.FIRST_NAME IS NOT NULL
  AND TRIM(rel.FIRST_NAME) <> ''

GROUP BY corp.SURNAME,
         ca.corporate_cust_id,
         corp.DATE_OF_BIRTH,
         id_country.DESCRIPTION,
         id.ID_NO,
         cc.ID_NO

ORDER BY corp.SURNAME;