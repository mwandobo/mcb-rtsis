create table RPT_AUDIT
(
    ID          BIGINT       not null
        constraint RPT_AUDIT_PK
            primary key,
    CREATED_ON  TIMESTAMP(6) not null,
    CREATED_BY  VARCHAR(50)  not null,
    TABLE_NAME  VARCHAR(50)  not null,
    FIELD_NAME  VARCHAR(50)  not null,
    RECORD_KEYS VARCHAR(100) not null,
    OLD_VALUE   VARCHAR(1000),
    NEW_VALUE   VARCHAR(1000),
    MAC_ADDRESS CHAR(12),
    HOST_NAME   VARCHAR(255),
    IP_ADDRESS  VARCHAR(15)
);

