create table DB_INFO
(
    DB_USER     VARCHAR(30) not null
        constraint PK_DBINF
            primary key,
    DB_NAME     VARCHAR(9),
    BD_ID       DECIMAL(15),
    DB_HOST     VARCHAR(64),
    DB_INS_NAME VARCHAR(16)
);

