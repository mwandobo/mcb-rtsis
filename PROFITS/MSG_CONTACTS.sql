create table MSG_CONTACTS
(
    ID              DECIMAL(12) not null
        constraint IXM_CON_001
            primary key,
    USER_NAME       VARCHAR(20) not null,
    FIRST_NAME      VARCHAR(50) not null,
    LAST_NAME       VARCHAR(50) not null,
    ADDRESS         VARCHAR(100),
    EMAIL           VARCHAR(100),
    TITLE           VARCHAR(100),
    COMPANY         VARCHAR(100),
    DEPARTMENT      VARCHAR(100),
    ROLE            VARCHAR(100),
    PHONE           VARCHAR(50),
    FAX             VARCHAR(50),
    CONNECTED       SMALLINT,
    CLIENT_IP       VARCHAR(16),
    CORE_BANKING_ID VARCHAR(50)
);

