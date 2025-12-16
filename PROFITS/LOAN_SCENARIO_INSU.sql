create table LOAN_SCENARIO_INSU
(
    INSTALL_SN            DECIMAL(10)    not null,
    TMSTAMP               TIMESTAMP(6)   not null,
    RECORD_SN             SMALLINT       not null,
    INSURANCE_PROD_ID     INTEGER,
    INSURANCE_VALUE       DECIMAL(15, 2) not null,
    USE_DRAWDOWN_AMN      CHAR(1),
    INSURED_PEOPLE        SMALLINT,
    CALCULATE_AMN         DECIMAL(15, 2),
    FIX_AMN_PAY           DECIMAL(15, 2),
    PERCENTAGE            DECIMAL(8, 4),
    INSURANCE_CHARGE_DESC VARCHAR(80),
    constraint PK_SCN_INS
        primary key (RECORD_SN, TMSTAMP, INSTALL_SN)
);

