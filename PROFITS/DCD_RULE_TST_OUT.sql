create table DCD_RULE_TST_OUT
(
    INTERNAL_SN      DECIMAL(10) not null,
    PRFT_SYSTEM      SMALLINT    not null,
    SN               DECIMAL(10) not null,
    VALRULE_ID       DECIMAL(12) not null,
    EXIT_STATE_ID    DECIMAL(12),
    OUTPUT_NUM_15_4  DECIMAL(15, 4),
    OUTPUT_DATE      DATE,
    OUTPUT_TIMESTAMP TIMESTAMP(6),
    CREATED_STAMP    TIMESTAMP(6),
    OUTPUT_FLAG_2    CHAR(2),
    FIELD_TYPE       CHAR(2),
    TCASE_ATTRIBUTE  CHAR(40),
    TCASE_TABLE      CHAR(40),
    OUTPUT_TEXT      VARCHAR(100),
    OUTPUT_TIME      TIME,
    constraint IXU_DEF_013
        primary key (INTERNAL_SN, PRFT_SYSTEM, SN, VALRULE_ID)
);

