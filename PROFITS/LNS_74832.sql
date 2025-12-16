create table LNS_74832
(
    TMSTAMP            TIMESTAMP(6) not null,
    REQUEST_LOAN_STS   CHAR(1)      not null,
    REQUEST_TYPE       CHAR(1)      not null,
    REQUEST_SN         SMALLINT     not null,
    ACC_TYPE           SMALLINT     not null,
    ACC_SN             INTEGER      not null,
    ACC_UNIT           INTEGER      not null,
    TOTAL_CONV_URL_AMN DECIMAL(15, 2),
    TOTAL_CRED_URL_AMN DECIMAL(15, 2),
    REQ_START_URL_BAL  DECIMAL(15, 2),
    REQ_CURR_URL_BAL   DECIMAL(15, 2),
    MAXIMUM_DATE       DATE,
    TRX_DATE           DATE,
    MINIMUM_DATE       DATE,
    REQUEST_STS        CHAR(1),
    CONVERTED_FLG      CHAR(1),
    constraint IXU_REP_057
        primary key (TMSTAMP, REQUEST_LOAN_STS, REQUEST_TYPE, REQUEST_SN, ACC_TYPE, ACC_SN, ACC_UNIT)
);

