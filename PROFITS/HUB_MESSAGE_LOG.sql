create table HUB_MESSAGE_LOG
(
    ID              INTEGER       not null
        constraint HUB_MESSAGE_LOG_PK
            primary key,
    TIMESTAMP       TIMESTAMP(6)  not null,
    SOURCE          VARCHAR(200)  not null,
    DESTINATION     VARCHAR(200)  not null,
    LOGICAL_KEY     VARCHAR(200),
    MESSAGE_TYPE    VARCHAR(200),
    SEC_LOGICAL_KEY VARCHAR(200),
    NODE_IP         VARCHAR(20),
    MESSAGE         BLOB(1048576) not null,
    PAYLOAD         BLOB(1048576),
    PROFITS_KEYS    VARCHAR(4000),
    HEADERS         CLOB(1048576),
    GROUPING        VARCHAR(40),
    PROCESS_STATUS  SMALLINT
);

