create table MSG_LOG_SQLSETUP
(
    ID          DECIMAL(12)   not null
        constraint IXM_LSS_001
            primary key,
    SOURCE_ID   DECIMAL(12)   not null,
    USER_NAME   VARCHAR(20),
    TABLE_NAME  VARCHAR(50),
    TABLE_FIELD VARCHAR(50),
    TIME_STAMP  TIMESTAMP(6)  not null,
    LOG_MESSAGE VARCHAR(2000) not null
);

