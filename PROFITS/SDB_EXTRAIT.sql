create table SDB_EXTRAIT
(
    SDB_UNIT           INTEGER        not null,
    SDB_SN             INTEGER        not null,
    SDB_RENTAL_DT      DATE           not null,
    TMSTAMP            TIMESTAMP(6)   not null,
    TRX_INTERNAL_SN    SMALLINT       not null,
    TRX_UNIT           INTEGER        not null,
    TRX_USR            CHAR(8),
    TRX_DATE           DATE,
    TRX_SN             INTEGER        not null,
    ENTRY_STATUS       CHAR(1)        not null,
    TRANSACTION_CODE   INTEGER        not null,
    JUSTIFICATION_CODE INTEGER        not null,
    TRX_AMN            DECIMAL(15, 2) not null,
    TRX_CURR           INTEGER        not null,
    RENTAL_AMN         DECIMAL(15, 2) not null,
    COMMISSION_AMN     DECIMAL(15, 2) not null,
    EXPENSE_AMN        DECIMAL(15, 2) not null,
    GUARRANTY_AMN      DECIMAL(15, 2) not null,
    CHR_IN_CC_AMN      DECIMAL(15, 2) not null,
    PRV_RENTAL_BAL     DECIMAL(15, 2) not null,
    COMMENTS           VARCHAR(40)    not null,
    CUST_ID            INTEGER,
    C_DIGIT            SMALLINT,
    constraint IXU_DEP_108
        primary key (SDB_UNIT, SDB_SN, SDB_RENTAL_DT, TMSTAMP, TRX_INTERNAL_SN)
);

