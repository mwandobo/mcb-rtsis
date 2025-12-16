create table TEST_HEADER
(
    HD_ID     SMALLINT       not null
        constraint I0000866
            primary key,
    HD_NAME   CHAR(20)       not null,
    HD_AMOUNT DECIMAL(15, 2) not null
);

