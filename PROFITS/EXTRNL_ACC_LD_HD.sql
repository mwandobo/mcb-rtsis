create table EXTRNL_ACC_LD_HD
(
    FILE_DATE      DATE         not null,
    FILE_SN        SMALLINT     not null,
    PROCESS_STATUS CHAR(1)      not null,
    PROCESS_ERROR  VARCHAR(300),
    SELECTION_STS  CHAR(1)      not null,
    ACC_INSERT_STS CHAR(1)      not null,
    TIMSTAMP       TIMESTAMP(6) not null,
    constraint PK_EXT_ACC_LD_HD
        primary key (FILE_SN, FILE_DATE)
);

