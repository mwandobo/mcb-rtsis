create table LOAN_SCENARIO_MED
(
    INSTALL_SN   DECIMAL(10)  not null,
    TMSTAMP      TIMESTAMP(6) not null,
    RECORD_SN    SMALLINT     not null,
    AGREEMENT_NO INTEGER      not null,
    constraint PK_SCN_MEDS
        primary key (RECORD_SN, TMSTAMP, INSTALL_SN)
);

