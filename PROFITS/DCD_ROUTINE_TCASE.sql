create table DCD_ROUTINE_TCASE
(
    PRFT_SYSTEM      SMALLINT    not null,
    ROUTINE_SN       DECIMAL(12) not null,
    ROUTINE_TEST_SN  INTEGER     not null,
    TEST_DESCRIPTION CHAR(40),
    ROUTINE_NAME     CHAR(80),
    FULL_DESCR       VARCHAR(2048),
    constraint IXU_DEF_010
        primary key (PRFT_SYSTEM, ROUTINE_SN, ROUTINE_TEST_SN)
);

