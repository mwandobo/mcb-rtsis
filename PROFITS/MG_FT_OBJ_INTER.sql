create table MG_FT_OBJ_INTER
(
    FILE_NAME          CHAR(50) not null,
    SERIAL_NO          INTEGER  not null,
    FT_STATUS          SMALLINT,
    MG_FT_OBJ_ISS_UNIT SMALLINT,
    FT_OBJ_NO_TAUT     INTEGER,
    MG_FT_OBJ_AMNT     DECIMAL(15, 2),
    FT_TMSTAMP         TIMESTAMP(6),
    FT_PROCESS_DATE    DATE,
    MG_FT_OBJ_ISS_DATE DATE,
    MG_FT_OBJ_STATUS   CHAR(1),
    FILE_DETAIL_ID     CHAR(2),
    MG_FT_OBJ_TYPE     CHAR(5),
    MG_FT_OBJ_ISS_CURR CHAR(5),
    MG_FT_OBJ_CURR     CHAR(5),
    FT_OBJ_NO          CHAR(10),
    MG_FT_OBJ_BENEF    CHAR(80),
    MG_FT_OBJ_ISSUER   CHAR(30),
    FT_ERR_DESC        CHAR(80),
    ACCOUNT_NO         CHAR(20) default ' ',
    constraint IXU_MIG_024
        primary key (FILE_NAME, SERIAL_NO)
);

create unique index MGFTOBJI_SEC1
    on MG_FT_OBJ_INTER (ACCOUNT_NO, FT_OBJ_NO);

