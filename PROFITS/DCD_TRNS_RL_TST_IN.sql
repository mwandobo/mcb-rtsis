create table DCD_TRNS_RL_TST_IN
(
    CODE               CHAR(8)      not null,
    INTERNAL_SN        DECIMAL(10)  not null,
    PRFT_SYSTEM        SMALLINT     not null,
    SN                 DECIMAL(10)  not null,
    TMPSTAMP           TIMESTAMP(6) not null,
    VALRULE_ID         DECIMAL(12)  not null,
    INPUT_NUMERIC_15_4 DECIMAL(15, 4),
    INPUT_TIMESTAMP    TIMESTAMP(6),
    INPUT_DATE         DATE,
    STATUS0            CHAR(1),
    MODIFIED_FLG       CHAR(1),
    INPUT_FLAG_2       CHAR(2),
    FIELD_TYPE         CHAR(2),
    PASSWORD           CHAR(26),
    TCASE_TABLE        CHAR(40),
    TEST_DESCR         CHAR(40),
    TCASE_ATTRIBUTE    CHAR(40),
    FULL_DESCR         CHAR(240),
    INPUT_TEXT         VARCHAR(100),
    INPUT_TIME         TIME,
    constraint IXU_DEF_124
        primary key (CODE, INTERNAL_SN, PRFT_SYSTEM, SN, TMPSTAMP, VALRULE_ID)
);

