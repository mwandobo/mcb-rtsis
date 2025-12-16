create table MSG_SEC_USER
(
    USERN_NAME VARCHAR(10) not null,
    TITLE      VARCHAR(10) not null,
    PASSWORD   VARCHAR(10) not null,
    PROFILE    VARCHAR(10) not null
);

create unique index MSG_SEC_USER_PK
    on MSG_SEC_USER (USERN_NAME);

