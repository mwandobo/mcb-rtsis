create table MG_COS_DIVIDENT
(
    FILE_NAME        CHAR(50) not null,
    SERIAL_NO        INTEGER  not null,
    ROW_STATUS       SMALLINT,
    MEMBER_ID        INTEGER,
    SHARE_ID         INTEGER,
    TAX_AMOUNT       DECIMAL(15, 2),
    DIVIDENT_AMOUNT  DECIMAL(15, 2),
    UTF_NUM1         DECIMAL(15, 2),
    UTF_NUM2         DECIMAL(15, 2),
    UTF_DATE1        DATE,
    DIVIDENT_DATE    DATE,
    ROW_PROCESS_DATE DATE,
    UTF_DATE2        DATE,
    LAST_ACQUIS_DATE CHAR(4),
    TRX_DIVIDENT_ACC CHAR(40),
    UTF_TEXT2        CHAR(80),
    UTF_TEXT1        CHAR(80),
    ROW_ERR_DESC     CHAR(80),
    constraint IXU_MIG_016
        primary key (FILE_NAME, SERIAL_NO)
);

