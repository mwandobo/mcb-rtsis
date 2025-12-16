create table LMS_MESSAGE_LOG
(
    TMSTAMP         TIMESTAMP(6) not null,
    TERMINAL_NUMBER CHAR(99)     not null,
    INTERNAL_SN     DECIMAL(13)  not null,
    SYSTEM_DATE     DATE,
    CURR_TRX_DATE   DATE,
    DAYS_BEFORE     SMALLINT,
    EXPIRY_DATE     DATE,
    SYSTEM_LOG      CHAR(80),
    ERROR_LOG       CHAR(80),
    constraint PK_LMS_999
        primary key (INTERNAL_SN, TERMINAL_NUMBER, TMSTAMP)
);

