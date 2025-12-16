create table DCD_RULE_TST_INPUT
(
    INTERNAL_SN        DECIMAL(10) not null,
    PRFT_SYSTEM        SMALLINT    not null,
    SN                 DECIMAL(10) not null,
    VALRULE_ID         DECIMAL(12) not null,
    INPUT_NUMERIC_15_4 DECIMAL(15, 4),
    INPUT_TIMESTAMP    TIMESTAMP(6),
    INPUT_DATE         DATE,
    MODIFIED_FLG       CHAR(1),
    FIELD_TYPE         CHAR(2),
    INPUT_FLAG_2       CHAR(2),
    TCASE_TABLE        CHAR(40),
    TEST_DESCR         CHAR(40),
    TCASE_ATTRIBUTE    CHAR(40),
    FULL_DESCR         CHAR(240),
    INPUT_TEXT         VARCHAR(100),
    INPUT_TIME         TIME,
    constraint IXU_DEF_059
        primary key (INTERNAL_SN, PRFT_SYSTEM, SN, VALRULE_ID)
);

