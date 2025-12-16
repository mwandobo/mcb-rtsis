create table CMS_EMBOSS_HDR
(
    FILE_SN            DECIMAL(15) not null
        constraint PK_CMS_EMBOSS_HDR
            primary key,
    FILE_NAME          CHAR(50),
    BATCH_NUMBER       INTEGER,
    RECORD_COUNT       DECIMAL(10),
    HEADER_FULL_LINE   VARCHAR(1000),
    TRAILER_FULL_LINE  VARCHAR(1000),
    COMPLETE_FILE_FLAG CHAR(1),
    PROGRAM_ID         CHAR(10),
    TMSTAMP            TIMESTAMP(6),
    PROCESS_RESULTS    CHAR(254),
    CURR_TRX_DATE      DATE,
    FILE_EXPORT_DATE   DATE
);

