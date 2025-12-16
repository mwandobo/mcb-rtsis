create table LOGGED_USER
(
    USERCODE           CHAR(8),
    SESSION_NO         SMALLINT,
    CASH_TILL_NO       INTEGER,
    UNITCODE           INTEGER,
    UNSUCC_PWD_COUNTER DECIMAL(15),
    PASSWORD_CHANGE_DA DATE,
    TMSTAMP            TIMESTAMP(6),
    LOGIN_STATUS       CHAR(1),
    PROFILE_2          CHAR(8),
    PROFILE_1          CHAR(8),
    PROFILE_3          CHAR(8),
    TERMINAL_ID        CHAR(99),
    MAC_ADDRESS        VARCHAR(20)
);

create unique index IXU_LOG_001
    on LOGGED_USER (USERCODE);

