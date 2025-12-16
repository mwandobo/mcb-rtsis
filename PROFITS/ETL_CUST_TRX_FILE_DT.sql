create table ETL_CUST_TRX_FILE_DT
(
    FILENAME          CHAR(50)       not null,
    TRX_DATE          DATE           not null,
    FILE_SN           SMALLINT       not null,
    LINE_NO           DECIMAL(10)    not null,
    RECORD_TYPE       CHAR(1),
    FILEID            CHAR(10),
    SETTL_YEAR_MONTH  INTEGER,
    INSTRUMENT        CHAR(3),
    TRX_YEAR_MONTH    INTEGER,
    TAX_ID            INTEGER,
    ACCOUNT_USAGE_FLG CHAR(4),
    SIGN_IND          CHAR(1),
    TOTAL_AMOUNT      DECIMAL(15, 2) not null,
    DETAIL_RECORDS_NO DECIMAL(10),
    GROUPS_NO         DECIMAL(10),
    FULL_LINE         VARCHAR(549),
    DATE_GROUP_HEADER CHAR(6),
    constraint PK_ETL_CFILE_DT
        primary key (LINE_NO, FILE_SN, TRX_DATE, FILENAME)
);

