create table HIST_LOGGED_USER
(
    USERCODE             CHAR(8)      not null,
    TMSTAMP              TIMESTAMP(6) not null,
    UNITCODE             INTEGER,
    CASH_TILL_NO         INTEGER,
    TERMINAL_ID          CHAR(99),
    SESSION_NO           SMALLINT,
    PASSWORD_CHANGE_DATE DATE,
    LOGIN_STATUS         CHAR(1),
    ID_TRANSACT          INTEGER,
    OLD_TILL             INTEGER,
    constraint PK_HIST_LOGGED_USER
        primary key (TMSTAMP, USERCODE)
);

