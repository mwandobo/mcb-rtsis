create table BENCHMARK_SCEN_DT
(
    BENCHMARK_ID      DECIMAL(15) not null,
    REVERSAL_JUSTIFIC CHAR(1),
    REQUEST_STATUS    CHAR(1),
    SCENARIO_DAY      SMALLINT    not null,
    SCENARIO_DATE     DATE        not null,
    SCENARIO_DESCR    CHAR(100),
    ACCOUNTS_SET      SMALLINT    not null,
    ACC_SET_DESCR     CHAR(100),
    STEP_COUNT        SMALLINT    not null,
    EXECUTE_STEP      CHAR(1),
    STEP_DESCR        CHAR(100),
    STEP_TRANSACTION  INTEGER     not null,
    STEP_JUSTIFIC     INTEGER,
    REVERSE_STEP      CHAR(1),
    EXECUTION_SYSTEM  SMALLINT    not null,
    LAST_STEP         CHAR(1),
    LAST_JUST_NOT_REV CHAR(1),
    constraint I0010660
        primary key (SCENARIO_DATE, STEP_COUNT, ACCOUNTS_SET, SCENARIO_DAY)
);

