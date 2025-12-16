create table LNS_74831
(
    TMSTAMP            TIMESTAMP(6) not null,
    REQUEST_LOAN_STS   CHAR(1)      not null,
    REQUEST_TYPE       CHAR(1)      not null,
    REQUEST_SN         SMALLINT     not null,
    ACC_TYPE           SMALLINT     not null,
    ACC_SN             INTEGER      not null,
    ACC_UNIT           INTEGER      not null,
    RL_REQUEST_CNT     INTEGER,
    RQ_URL_PNL_INT_BAL DECIMAL(15, 2),
    RQ_FOR_PNL_INT_BAL DECIMAL(15, 2),
    RQ_FOR_NRM_INT_BAL DECIMAL(15, 2),
    RQ_URL_NRM_INT_BAL DECIMAL(15, 2),
    MAXIMUM_DATE       DATE,
    MINIMUM_DATE       DATE,
    TRX_DATE           DATE,
    REQUEST_STS        CHAR(1),
    RL_URL_DURATION    CHAR(1),
    HAS_REALIZED_INT   CHAR(1),
    constraint IXU_REP_208
        primary key (TMSTAMP, REQUEST_LOAN_STS, REQUEST_TYPE, REQUEST_SN, ACC_TYPE, ACC_SN, ACC_UNIT)
);

