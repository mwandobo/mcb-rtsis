create table LOAN_SCENARIO_SPR
(
    INSTALL_SN INTEGER      not null,
    TMSTAMP    TIMESTAMP(6) not null,
    RECORD_SN  INTEGER      not null,
    SPR_DATE   DATE,
    SPR_RATE   DECIMAL(12, 6)
);

create unique index LOAN_SCEN_SPR
    on LOAN_SCENARIO_SPR (INSTALL_SN, TMSTAMP, RECORD_SN);

