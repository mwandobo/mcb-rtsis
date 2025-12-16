create table LNS_URL_CORR_74862
(
    ACC_UNIT         INTEGER        not null,
    ACC_SN           INTEGER        not null,
    ACC_TYPE         SMALLINT       not null,
    REQUEST_SN       SMALLINT       not null,
    REQUEST_TYPE     CHAR(1)        not null,
    REQUEST_LOAN_STS CHAR(1)        not null,
    PRFT_ACC_NUM     CHAR(40)       not null,
    PRFT_SYSTEM      SMALLINT       not null,
    PRFT_ACC_CD      SMALLINT       not null,
    HOLD_NRM_RL_ACCR DECIMAL(15, 2) not null,
    P_RQ_URL_INT_BAL DECIMAL(15, 2) not null,
    O_RQ_URL_INT_BAL DECIMAL(15, 2) not null,
    RECORD_STS       CHAR(1)        not null,
    RECORD_PROC_DESC CHAR(80),
    TMSTAMP          TIMESTAMP(6),
    constraint CORR74862PK
        primary key (ACC_UNIT, ACC_SN, ACC_TYPE, REQUEST_SN, REQUEST_TYPE, REQUEST_LOAN_STS)
);

