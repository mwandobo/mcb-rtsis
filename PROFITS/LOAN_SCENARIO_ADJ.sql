create table LOAN_SCENARIO_ADJ
(
    INSTALL_SN        INTEGER      not null,
    TMSTAMP           TIMESTAMP(6) not null,
    RECORD_SN         INTEGER      not null,
    ADJ_REQ_INST_FROM INTEGER,
    ADJ_REQ_INST_TO   INTEGER,
    ADJ_FIXED_AMN     DECIMAL(15, 2),
    ZERO_INT_FLG      CHAR(1)
);

create unique index LOAN_SCEN_ADJ
    on LOAN_SCENARIO_ADJ (INSTALL_SN, TMSTAMP, RECORD_SN);

