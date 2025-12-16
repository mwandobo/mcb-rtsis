create table LOG_GENERIC_PROCESS
(
    TRX_DATE       DATE         not null,
    TRX_USR        CHAR(8)      not null,
    ACCOUNT_NUMBER CHAR(40)     not null,
    TMSTAMP        TIMESTAMP(6) not null,
    ERROR_LOG      CHAR(80),
    constraint PK_LOG_ERROR_36670
        primary key (TMSTAMP, ACCOUNT_NUMBER, TRX_USR, TRX_DATE)
);

