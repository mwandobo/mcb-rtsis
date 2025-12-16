create table TEST_DETAILS
(
    DT_ID              SMALLINT       not null
        constraint I0000863
            primary key,
    DT_NAME            CHAR(20)       not null,
    DT_AMOUNT_1        DECIMAL(15, 2) not null,
    DT_AMOUNT_2        DECIMAL(15, 2),
    DT_FLAG            CHAR(1)        not null,
    FK_TEST_HEADERHD_I SMALLINT
);

