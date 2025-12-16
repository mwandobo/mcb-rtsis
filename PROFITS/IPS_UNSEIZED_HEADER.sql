create table IPS_UNSEIZED_HEADER
(
    FILENAME      CHAR(50) not null
        constraint PK_UNSEIZ_HDR
            primary key,
    TRX_DATE      DATE     not null,
    COMPLETE_FLAG CHAR(1),
    TMSTAMP       TIMESTAMP(6),
    EXP_FILE_FLAG CHAR(1),
    EXP_FILENAME  CHAR(50),
    EXP_FILE_SN   SMALLINT,
    EXP_DATE      DATE,
    EXP_TIMESTAMP TIMESTAMP(6)
);

