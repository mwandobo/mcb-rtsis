create table PAT_USER_PROFILE
(
    ID                CHAR(10)  not null
        constraint PATMNPK1
            primary key,
    DESCRIPTION       CHAR(32)  not null,
    BANK_NAME         CHAR(10)  not null,
    DB_CONNECT_STRING CHAR(240) not null,
    APP_ARGUMENTS     CHAR(80),
    ACCESS_RIGHTS     SMALLINT,
    TESTER_NAME       CHAR(60),
    CLIENT_TYPE       CHAR(1),
    LANGUAGE_CODE     CHAR(2),
    VALUE_OWNERSHIP   CHAR(1)   not null,
    STATUS            CHAR(1)   not null
);

