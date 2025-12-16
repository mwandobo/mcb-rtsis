create table LOAN_INTER_RESULT
(
    ACC_UNIT            INTEGER      not null,
    ACC_TYPE            SMALLINT     not null,
    ACC_SN              INTEGER      not null,
    TRX_UNIT            INTEGER      not null,
    TRX_DATE            DATE         not null,
    TRX_USR             CHAR(8)      not null,
    TRX_SN              INTEGER      not null,
    ACC_LOAN_STATUS     CHAR(1)      not null,
    TMSTAMP             TIMESTAMP(6) not null,
    CALC_DAYS           SMALLINT,
    REQUEST_SN          SMALLINT,
    TRX_CODE            INTEGER,
    TRX_JUSTIFICATION   INTEGER,
    TOT_DB_INTEREST_AM  DECIMAL(15, 2),
    TOT_SUBS_INTEREST   DECIMAL(15, 2),
    TOT_N128_INTEREST   DECIMAL(15, 2),
    TOT_PEN_INTEREST_A  DECIMAL(15, 2),
    TOT_SPRD_INTEREST   DECIMAL(15, 2),
    TOT_S2_INTEREST_AMN DECIMAL(15, 2),
    TOT_CR_INTEREST_AMN DECIMAL(15, 2),
    TOT_PRODUCT_AMN     DECIMAL(15, 2),
    TOT_EXTRA_INT_AMN   DECIMAL(15, 2),
    TOT_CRSPRD_INT_AMN  DECIMAL(15, 2),
    VALEUR_DT           DATE,
    LST_ACR_CALC_DT     DATE,
    REQUEST_LOAN_STS    CHAR(1),
    REQUEST_TYPE        CHAR(1),
    REVERSAL_FLG        CHAR(1),
    CALC_INTER_STATUS   CHAR(1),
    REQUEST_STS         CHAR(1),
    constraint IXU_LOA_049
        primary key (ACC_UNIT, ACC_TYPE, ACC_SN, TRX_UNIT, TRX_DATE, TRX_USR, TRX_SN, ACC_LOAN_STATUS, TMSTAMP)
);

create unique index IXN_LOA_027
    on LOAN_INTER_RESULT (TRX_DATE, ACC_UNIT);

