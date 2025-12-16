create table MOF_SDOE_CUST_HD
(
    RECORD_TYPE        CHAR(2),
    BATCH_ID           CHAR(12) not null,
    FILE_ID            SMALLINT not null,
    CUST_TIMESTMP      CHAR(26),
    RESPON_BANK_CD     CHAR(3),
    TARGET_BANK_CD     CHAR(3),
    DETAIL_RECS        CHAR(12),
    FILE_TYPE          CHAR(2),
    RESPON_VERSION_ID  CHAR(12),
    REPLACE_VERSION_ID CHAR(12),
    TOTAL_FILES        CHAR(4),
    FILENAME           CHAR(50),
    LAST_FILE_FLG      CHAR(1),
    ERROR_FIELD        CHAR(2),
    ERROR_CODE         CHAR(2),
    constraint PK_CUST_HD
        primary key (FILE_ID, BATCH_ID)
);

