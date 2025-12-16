create table DCD_SCENARIO_DTL
(
    SCENARIO_ID      DECIMAL(15) not null,
    RULE_SYSTEM      SMALLINT    not null,
    RULE_ID          DECIMAL(12) not null,
    TEST_CASE_ID     DECIMAL(10) not null,
    INTERNAL_SN      DECIMAL(15) not null,
    SCENARIO_TMSTAMP TIMESTAMP(6),
    constraint IXU_DEF_060
        primary key (SCENARIO_ID, RULE_SYSTEM, RULE_ID, TEST_CASE_ID, INTERNAL_SN)
);

