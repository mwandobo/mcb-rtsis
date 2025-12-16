create table REP_74990
(
    REC_NUM          DECIMAL(10) not null
        constraint IXU_REP_036
            primary key,
    INTEREST_DAYS    SMALLINT,
    ACTIVITY         SMALLINT,
    TOTAL_INTEREST   DECIMAL(9, 6),
    SUBSIDY_INTEREST DECIMAL(9, 6),
    REQ_SUBDIDY_AMN  DECIMAL(15, 2),
    INITIAL_AMN      DECIMAL(15, 2),
    NORMAL_BAL       DECIMAL(15, 2),
    TOTAL_RATE_AMN   DECIMAL(15, 2),
    AFM              CHAR(20),
    AGR_NUMBER       CHAR(25),
    COUNTRY          CHAR(30),
    ACCOUNT_NO       CHAR(30),
    BENEF_SURNAME    CHAR(50),
    GUAR_DESISION    CHAR(50),
    INITIAL_KYA      CHAR(60)
);

