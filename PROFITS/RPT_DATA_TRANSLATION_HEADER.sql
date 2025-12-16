create table RPT_DATA_TRANSLATION_HEADER
(
    ID          INTEGER      not null,
    BASE        VARCHAR(100) not null,
    TABLE_NAME  VARCHAR(100),
    COLUMN_NAME VARCHAR(50),
    CREATED     TIMESTAMP(6) not null,
    CREATED_BY  VARCHAR(50)  not null,
    UPDATED     TIMESTAMP(6) not null,
    UPDATED_BY  VARCHAR(50)  not null
);

