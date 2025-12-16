create table WRH_ERROR_LOG
(
    TMSTMP         TIMESTAMP(6) not null
        constraint IXU_EOM_010
            primary key,
    PRFT_SYSTEM    SMALLINT,
    CREATION_DT    DATE,
    PROGRAM_ID     CHAR(5),
    ACCOUNT_NUMBER CHAR(40),
    ERROR_MSG      CHAR(100)
);

