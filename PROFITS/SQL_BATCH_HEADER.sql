create table SQL_BATCH_HEADER
(
    HEADER_ID   DECIMAL(15) not null
        constraint PK_SQL_HEADER
            primary key,
    NAME        CHAR(200)   not null,
    DESCRIPTION VARCHAR(200)
);

