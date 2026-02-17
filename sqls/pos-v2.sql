SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')    AS reportingDate,
       201                                                  AS posBranchCode,
       at.FK_USRCODE                                        AS posNumber,
       'FSR-' || CAST(at.FK_USRCODE AS VARCHAR(10))         AS qrFsrCode,
       'Bank Agent'                                         AS posHolderCategory,
       'Selcom'                                             AS posHolderName,
       NULL                                                 AS posHolderNin,
       '103-847-451'                                        AS posHolderTin,
       NULL                                                 AS postalCode,
       al.REGION                                            AS region,
       al.DISTRICT                                          AS district,
       al.WARD                                              AS ward,
       'N/A'                                                AS street,
       'N/A'                                                AS houseNumber,
       al.GPS_COORDINATES                                   AS gpsCoordinates,
       '230000070'                                          AS linkedAccount,
       VARCHAR_FORMAT(at.INSERTION_TMSTAMP, 'DDMMYYYYHHMM') AS issueDate,
       NULL                                                 AS returnDate
FROM AGENT_TERMINAL at
         LEFT JOIN AGENTS_LIST_V3 al
                   ON
                       CASE
                           WHEN LENGTH(REPLACE(at.FK_USRCODE, ' ', '')) > 8
                               THEN RIGHT(REPLACE(at.FK_USRCODE, ' ', ''), 8)
                           ELSE REPLACE(at.FK_USRCODE, ' ', '')
                           END
                           =
                       CASE
                           WHEN LENGTH(REPLACE(al.TERMINAL_ID, ' ', '')) > 8
                               THEN RIGHT(REPLACE(al.TERMINAL_ID, ' ', ''), 8)
                           ELSE REPLACE(al.TERMINAL_ID, ' ', '')
                           END
