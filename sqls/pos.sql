SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')    AS reportingDate,
    201                                                  AS posBranchCode,
    at.FK_USRCODE                                        AS posNumber,
    'FSR-' || CAST(at.FK_USRCODE AS VARCHAR(10))         AS qrFsrCode,
    'Selcom Paytech Ltd'                                 AS posHolderName,
    NULL                                                 AS posHolderNin,
    '103847451'                                          AS posHolderTin,
    NULL                                                 AS postalCode,
    dl.REGION                                           AS region,
    dl.DISTRICT                                         AS district,
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