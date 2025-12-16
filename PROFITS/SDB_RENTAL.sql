create table SDB_RENTAL
(
    RENTAL_DT          DATE    not null,
    RENTAL             DECIMAL(15, 2),
    RENTAL_CURRENCY    INTEGER not null,
    RENTAL_EXPIRY_DT   DATE,
    RENTAL_RENEWAL_DT  DATE,
    DEP_ACC_UNIT       INTEGER,
    DEP_ACC_TYPE       SMALLINT,
    DEP_ACC_SN         DECIMAL(11),
    DEP_ACC_CD         SMALLINT,
    TOT_RENTAL         DECIMAL(15, 2),
    TOT_COMISSION      DECIMAL(15, 2),
    TOT_EXPENSE        DECIMAL(15, 2),
    RENTAL_BAL         DECIMAL(15, 2),
    COMISSION_BAL      DECIMAL(15, 2),
    EXPENSE_BAL        DECIMAL(15, 2),
    GUARANTEE_BAL      DECIMAL(15, 2),
    LAST_TRX_DT        DATE,
    LAST_TRX_USR       CHAR(8),
    TRX_CNT            SMALLINT,
    RENTAL_COMMENTS    VARCHAR(240),
    TMSTAMP            TIMESTAMP(6),
    RENTAL_STATUS      CHAR(1),
    FKSDB_UNIT         INTEGER not null,
    FKSDB_SN           INTEGER not null,
    FK_CUST_ID         INTEGER,
    FK_CUST_ADDR_SN    SMALLINT,
    FK_CURRENCYID_CURR INTEGER,
    BLOCK_REASON       VARCHAR(240),
    constraint IXU_DEP_109
        primary key (FKSDB_UNIT, FKSDB_SN, RENTAL_DT)
);

