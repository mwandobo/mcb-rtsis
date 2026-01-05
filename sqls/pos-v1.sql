SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')    AS reportingDate,
       201                                                  AS posBranchCode,
       at.FK_USRCODE                                        AS posNumber,
       'FSR-' || CAST(at.FK_USRCODE AS VARCHAR(10))         AS qrFsrCode,
       'Selcom Paytech Ltd'                                 AS posHolderName,
       NULL                                                 AS posHolderNin,
       '103847451'                                          AS posHolderTin,
       NULL                                                 AS postalCode,
       COALESCE(
               al.REGION,
               dl.REGION,
               (SELECT r.REGION
                FROM PROFITS.BANK_LOCATION_LOOKUP r
                WHERE UPPER(TRIM(at.LOCATION)) LIKE '%' || UPPER(TRIM(r.REGION)) || '%'
                    FETCH FIRST 1 ROW ONLY),
               'N/A'
       )                                                    AS region,
       COALESCE(
               al.DISTRICT,
               dl.DISTRICT,
               (SELECT r.DISTRICT
                FROM PROFITS.BANK_LOCATION_LOOKUP r
                WHERE UPPER(TRIM(at.LOCATION)) LIKE '%' || UPPER(TRIM(r.DISTRICT)) || '%'
                    FETCH FIRST 1 ROW ONLY),
               'N/A'
       )                                                    AS district,
       'N/A'                                                AS ward,
       'N/A'                                                AS street,
       'N/A'                                                AS houseNumber,
       al.GPS                                               AS gpsCoordinates,
       '230000070'                                          AS linkedAccount,
       VARCHAR_FORMAT(at.INSERTION_TMSTAMP, 'DDMMYYYYHHMM') AS issueDate,
       NULL                                                 AS returnDate
FROM PROFITS.AGENT_TERMINAL at
         LEFT JOIN AGENTS_LIST al
                   ON RIGHT(TRIM(al.TERMINAL_ID), 8) = TRIM(at.FK_USRCODE)
         LEFT JOIN PROFITS.BANK_LOCATION_LOOKUP dl
                   ON UPPER(TRIM(at.LOCATION)) LIKE '%' || UPPER(dl.DISTRICT) || '%';