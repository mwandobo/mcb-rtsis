create table ELTA_DTL_TLF
(
    RECORD_SN          DECIMAL(10) not null,
    TC_CODE            DECIMAL(15) not null,
    CUST_ID            INTEGER,
    ACCOUNT_NUMBER     DECIMAL(11),
    ACCOUNT_CD         SMALLINT,
    TRX_CCY_DESC       CHAR(3),
    TRX_AMOUNT         DECIMAL(15, 2),
    TRX_DATE           DATE,
    TRX_UNIT           INTEGER,
    TRX_USER           CHAR(8),
    TRX_USER_SN        INTEGER,
    TRX_USER_INT_SN    SMALLINT,
    TRX_TMSTAMP        TIMESTAMP(6),
    SETTLE_DATE        DATE,
    SETTLE_UNIT        INTEGER,
    SETTLE_USR         CHAR(8),
    SETTLE_USER_SN     INTEGER,
    SETTLE_USER_INT_SN SMALLINT,
    SETTLE_TMSTAMP     TIMESTAMP(6),
    SETTLE_COMMENTS    CHAR(80),
    TMSTAMP            TIMESTAMP(6),
    PROCESS_DATE       DATE,
    RECONCILE_STS      CHAR(1)     not null,
    COMMENTS           CHAR(30),
    FK_ELTAHDTLF_SN    DECIMAL(10) not null,
    FK_ELTAHDTLF_TYPE  CHAR(1)     not null,
    FK_ELTAHDTLF_DATE  DATE        not null,
    PREV_RECONC_STS    CHAR(1),
    constraint PK_ELTA_DTL_TLF
        primary key (FK_ELTAHDTLF_DATE, FK_ELTAHDTLF_TYPE, FK_ELTAHDTLF_SN, RECORD_SN)
);

