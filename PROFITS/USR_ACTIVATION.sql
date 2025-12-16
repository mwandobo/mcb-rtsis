create table USR_ACTIVATION
(
    TMSTAMP           TIMESTAMP(6) not null
        constraint PK_USR_ACTIVATION
            primary key,
    TRX_USR_CODE      CHAR(8)      not null,
    AFFECTED_USR_CODE CHAR(8)      not null,
    PROCESS_TYPE      SMALLINT     not null
);

