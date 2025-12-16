create table MSG_AUDIT
(
    ID          DECIMAL(12)  not null
        constraint MSG_AUDIT_PK
            primary key,
    CREATED_ON  TIMESTAMP(6) not null,
    CREATED_BY  VARCHAR(50)  not null,
    TABLE_NAME  VARCHAR(50)  not null,
    FIELD_NAME  VARCHAR(50)  not null,
    RECORD_KEYS VARCHAR(100) not null,
    OLD_VALUE   VARCHAR(1000),
    NEW_VALUE   VARCHAR(1000)
);

