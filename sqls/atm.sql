SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                    AS reportingDate,
       be.FIRST_NAME                                                        AS atmName,
       b.branchCode,
       be.STAFF_NO                                                          AS atmCode,
       NULL                                                                 AS tillNumber,
       'M-Pesa'                                                             AS mobileMoneyServices,
       'FSR-' || CAST(be.STAFF_NO AS VARCHAR(10))                           AS qrFsrCode,
       NULL                                                                 AS postalCode,
       'DAR ES SALAAM'                                                      AS region,
       CASE WHEN b.branchCode = 200 THEN 'ILALA' ELSE 'UBUNGO' END          AS district,
       CASE WHEN b.branchCode = 200 THEN 'KISUTU' ELSE 'UBUNGO WARD' END    AS ward,
       CASE WHEN b.branchCode = 200 THEN 'SAMORA STREET' ELSE 'MLIMANI' END AS street,
       NULL                                                                 AS houseNumber,
       CASE
           WHEN u.LATITUDE_LOCATION IS NOT NULL AND u.LONGITUDE_LOCATION IS NOT NULL
               THEN TRIM(u.LATITUDE_LOCATION) || ',' || TRIM(u.LONGITUDE_LOCATION)
           WHEN u.GEO_AREA IS NOT NULL
               THEN u.GEO_AREA
           ELSE '0.0000,0.0000' -- Default coordinates - should be updated
           END                                                              AS gpsCoordinates,
       CASE WHEN b.branchCode = 200 THEN '101000010' ELSE '101000015' END   AS linkedAccount,
       VARCHAR_FORMAT(be.TMSTAMP, 'DDMMYYYYHHMM')                           AS openingDate,
       'active'                                                             AS atmStatus,
       null                                                                 AS closureDate,
       'Card and Mobile Based'                                              AS atmChannel
FROM BANKEMPLOYEE be
         JOIN (SELECT STAFF_NO,
                      CASE
                          WHEN STAFF_NO = 'MWL01001' THEN 200
                          ELSE 201
                          END AS branchCode
               FROM BANKEMPLOYEE) b
              ON b.STAFF_NO = be.STAFF_NO
         JOIN UNIT u
              ON u.CODE = b.branchCode
WHERE be.STAFF_NO IS NOT NULL
  AND be.STAFF_NO = TRIM(be.STAFF_NO)
  AND be.EMPL_STATUS = 1
  AND be.STAFF_NO LIKE 'MWL01%'
  AND u.CODE IN (200, 201);
