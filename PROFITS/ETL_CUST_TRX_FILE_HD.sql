create table ETL_CUST_TRX_FILE_HD
(
    FILENAME      CHAR(50) not null,
    TRX_DATE      DATE     not null,
    FILE_SN       SMALLINT not null,
    COMPLETE_FLAG CHAR(1),
    TMSTAMP       TIMESTAMP(6),
    constraint PK_ETL_CFILE_HDR
        primary key (FILE_SN, TRX_DATE, FILENAME)
);

