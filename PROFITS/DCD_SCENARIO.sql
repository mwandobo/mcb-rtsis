create table DCD_SCENARIO
(
    SCENARIO_ID      DECIMAL(15) not null
        constraint IXU_DEF_120
            primary key,
    SCENARIO_DESC    CHAR(40),
    SCENARIO_SPECS_2 VARCHAR(2048),
    SCENARIO_SPECS_1 VARCHAR(2048)
);

