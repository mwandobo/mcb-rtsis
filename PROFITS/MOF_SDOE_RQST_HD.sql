create table MOF_SDOE_RQST_HD
(
    RECORD_TYPE        CHAR(2),
    BATCH_ID           CHAR(12) not null
        constraint PK_SDOE_RQST_HD
            primary key,
    REQUEST_TYPE       CHAR(2),
    REQST_TIMESTMP     CHAR(26),
    TOTAL_REQUESTS     INTEGER,
    FILENAME           CHAR(50),
    BANK_ID            CHAR(3),
    PROCESSED_FLG      CHAR(1),
    APTLSMATA_FILE_FLG CHAR(1),
    PROIONTA_FILE_FLG  CHAR(1),
    PELATES_FILE_FLG   CHAR(1),
    SXESEIS_FILE_FLG   CHAR(1),
    KINHSEIS_FILE_FLG  CHAR(1)
);

