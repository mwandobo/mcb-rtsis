create table EXT_CUSTPOS_LD_HD
(
    FILE_DATE       DATE         not null,
    FILE_SN         SMALLINT     not null,
    PROCESS_STATUS  CHAR(1)      not null,
    PROCESS_ERROR   VARCHAR(300),
    SELECTION_STS   CHAR(1)      not null,
    CUSTPOS_INS_STS CHAR(1)      not null,
    TIMSTAMP        TIMESTAMP(6) not null,
    constraint PK_EXT_CUSTPOS_HD
        primary key (FILE_SN, FILE_DATE)
);

