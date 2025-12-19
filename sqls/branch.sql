-- Branch Information RTSIS Report
SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       COALESCE(
               u.UNIT_NAME,
               u.UNIT_NAME_LATIN,
               'Unknown Branch'
       )                                                 AS branchName,
       117039447                                         AS taxIdentificationNumber,
       CASE
           WHEN u.CODE = 201 THEN 'BL20000102884'
           WHEN u.CODE = 200 THEN 'BL20000102884'
           END                                           AS businessLicense,
       CAST(u.CODE AS VARCHAR(10))                       AS branchCode,
       'FSR-' || CAST(u.CODE AS VARCHAR(10))    AS qrFsrCode,
       U.ADDRESS_2                                       AS region,
       CASE
           WHEN u.CODE = 201 THEN 'Ubungo'
           WHEN u.CODE = 200 THEN 'Ilala'
           END                                           AS district,
       CASE
           WHEN u.CODE = 201 THEN 'Sinza'
           WHEN u.CODE = 200 THEN 'Kisutu'
           END                                           AS ward,
       CASE
           WHEN u.CODE = 201 THEN 'Sam Nujoma Road'
           WHEN u.CODE = 200 THEN 'Samora Avenue'
           END                                           AS street,
       null                                              AS houseNumber,
       u.ADDRESS_LATIN                                   AS postalCode,
       CASE
           WHEN u.LATITUDE_LOCATION IS NOT NULL AND u.LONGITUDE_LOCATION IS NOT NULL
               THEN TRIM(u.LATITUDE_LOCATION) || ',' || TRIM(u.LONGITUDE_LOCATION)
           WHEN u.GEO_AREA IS NOT NULL
               THEN u.GEO_AREA
           ELSE '0.0000,0.0000' -- Default coordinates - should be updated
           END                                           AS gpsCoordinates,

       CASE
           WHEN u.CODE = 201 OR U.CODE = 200 THEN 'Fully fledged'
           ELSE 'Service center'
           END                                           AS bankingServices,
       'M-Pesa Airtel Money Tigo Pesa Halopesa'          AS mobileMoneyServices,
       VARCHAR_FORMAT(
               DATE(u.TMSTAMP),
               'DDMMYYYYHHMM'
       )                                                 AS registrationDate,
       CASE
           WHEN u.ENTRY_STATUS = '1' AND u.INACTIVE_UNIT != '0' THEN 'Active'
           WHEN u.ENTRY_STATUS = '1' AND u.INACTIVE_UNIT = '0' THEN 'Inactive'
           WHEN u.ENTRY_STATUS = '0' THEN 'Closed'
           ELSE 'Unknown'
           END                                           AS branchStatus,
       null                                              AS closureDate,
       CASE
           WHEN u.CODE = 201 THEN 'Furahini S. Lema'
           WHEN u.CODE = 200 THEN 'Monica G. Malisa'
           END                                           AS contactPerson,
       CASE
           WHEN u.CODE = 201 THEN '0713249528'
           WHEN u.CODE = 200 THEN '0682697276'
           END                                           AS telephoneNumber,
       CASE
           WHEN u.CODE = 201 THEN '0756818609'
           WHEN u.CODE = 200 THEN '0682697276'
           END                                           AS altTelephoneNumber,
       'Brick and Mortar'                                AS branchCategory
FROM UNIT u WHERE
    u.UNIT_NAME = 'MLIMANI BRANCH'
               OR u.UNIT_NAME = 'SAMORA BRANCH'
ORDER BY u.CODE;