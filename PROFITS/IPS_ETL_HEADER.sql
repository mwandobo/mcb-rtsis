create table IPS_ETL_HEADER
(
    FILENAME      CHAR(50) not null,
    TRX_DATE      DATE     not null,
    FILE_TYPE     CHAR(1)  not null,
    COMPLETE_FLAG CHAR(1),
    EXP_FILE_FLAG CHAR(1),
    TMSTAMP       TIMESTAMP(6),
    EXP_FILENAME  CHAR(50),
    EXP_DATE      DATE,
    EXP_FILE_ID   CHAR(35),
    EXP_TIMESTAMP TIMESTAMP(6),
    EXP_FILE_SN   SMALLINT,
    constraint PK_ETL_HDR
        primary key (FILENAME, FILE_TYPE)
);

