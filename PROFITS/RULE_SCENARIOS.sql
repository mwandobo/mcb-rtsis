create table RULE_SCENARIOS
(
    RULE_TYPE              CHAR(20)    not null,
    RULE_SN                DECIMAL(10) not null,
    RULE_SCENARIO_DESC     VARCHAR(80),
    RULE_SCENARIO          VARCHAR(4000),
    RULE_SCENARIO_ANALYSIS VARCHAR(4000),
    constraint PK_RULE_SCENARIO
        primary key (RULE_SN, RULE_TYPE)
);

