create table CORR_RECONCILE_TMP
(
    ENTRY_SER_NUM  SMALLINT     not null,
    TRANS_SER_NUM  DECIMAL(10)  not null,
    ACCOUNT_NUMBER DECIMAL(11)  not null,
    TABLE_FLAG     CHAR(1)      not null,
    TMSTAMP        TIMESTAMP(6) not null,
    EXPECTED_REF   DECIMAL(10),
    EQUALITY_IND   CHAR(2),
    POST_FLAG      CHAR(1),
    constraint IXU_REP_041
        primary key (ENTRY_SER_NUM, TRANS_SER_NUM, ACCOUNT_NUMBER, TABLE_FLAG, TMSTAMP)
);

