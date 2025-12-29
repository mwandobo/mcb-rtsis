SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')    AS reportingDate,
    201                                                  AS posBranchCode,
    at.FK_USRCODE                                        AS posNumber,
    'FSR-' || CAST(at.FK_USRCODE AS VARCHAR(10))         AS qrFsrCode,
    'Selcom Paytech Ltd'                                 AS posHolderName,
    NULL                                                 AS posHolderNin,
    '103847451'                                          AS posHolderTin,
    NULL                                                 AS postalCode,
    COALESCE(
            dl.REGION,
            (SELECT r.REGION
             FROM PROFITS.BANK_LOCATION_LOOKUP r
             WHERE UPPER(TRIM(at.LOCATION)) LIKE '%' || UPPER(r.REGION) || '%'
                 FETCH FIRST 1 ROW ONLY),
            'DAR ES SALAAM'  -- final hardcoded fallback
    ) AS region,
    COALESCE(
            dl.DISTRICT,
            (SELECT r.DISTRICT
             FROM PROFITS.BANK_LOCATION_LOOKUP r
             WHERE UPPER(TRIM(at.LOCATION)) LIKE '%' || UPPER(r.REGION) || '%'
                 FETCH FIRST 1 ROW ONLY),
            'ILALA'          -- final hardcoded fallback
    ) AS district,
    NULL                                                AS ward,
    NULL                                                AS street,
    NULL                                                AS houseNumber,
    NULL                                                AS gpsCoordinates,
    '230000070'                                        AS linkedAccount,
    VARCHAR_FORMAT(at.INSERTION_TMSTAMP, 'DDMMYYYYHHMM') AS issueDate,
    NULL                                               AS returnDate
FROM PROFITS.AGENT_TERMINAL at
         LEFT JOIN PROFITS.BANK_LOCATION_LOOKUP dl
                   ON UPPER(TRIM(at.LOCATION)) LIKE '%' || UPPER(dl.DISTRICT) || '%';
