create table KONDOR_DAILY_TIME_DEPS
(
    REC_SN            INTEGER not null,
    REC_DATE          DATE    not null,
    REC_TMSTAMP       TIMESTAMP(6),
    FIN_TYPE          CHAR(1),
    COUNTER_PARTY     INTEGER,
    TIME_DEP_TRX_DATE DATE,
    TIME_DEP_VAL_DATE DATE,
    TIME_DEP_MAT_DATE DATE,
    RENEWAL_AMT       DECIMAL(15, 2),
    CL_RATE           DECIMAL(12, 6),
    CL_MARGIN         INTEGER,
    constraint PK_KNDTDEP
        primary key (REC_DATE, REC_SN)
);

