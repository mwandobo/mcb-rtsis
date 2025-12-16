create table DCD_TRNS_ROUT_TCAS
(
    CODE             CHAR(8)      not null,
    PRFT_SYSTEM      SMALLINT     not null,
    ROUTINE_SN       DECIMAL(12)  not null,
    ROUTINE_TEST_SN  INTEGER      not null,
    TMPSTAMP         TIMESTAMP(6) not null,
    STATUS0          CHAR(1),
    PASSWORD         CHAR(26),
    TEST_DESCRIPTION CHAR(40),
    ROUTINE_NAME     CHAR(80),
    FULL_DESCR       VARCHAR(2048),
    constraint IXU_DEF_125
        primary key (CODE, PRFT_SYSTEM, ROUTINE_SN, ROUTINE_TEST_SN, TMPSTAMP)
);

