create table ATM_TRX_RECON
(
    ACCOUNT_NUMBER     CHAR(40) not null,
    TRX_UNIT           INTEGER  not null,
    TRX_DATE           DATE     not null,
    TRX_USR            CHAR(8)  not null,
    TRX_USR_SN1        INTEGER  not null,
    TRX_USR_SN2        INTEGER,
    TRX_USR_SN3        INTEGER,
    PROCESSING_CODE    CHAR(6),
    DEP_ACC_NUMBER     DECIMAL(11),
    REVERSED_FLAG      CHAR(1),
    TMSTAMP            TIMESTAMP(6),
    RECON_FLAG         CHAR(1),
    RECONCILED_TMSTAMP TIMESTAMP(6),
    REV_TMSTAMP        TIMESTAMP(6),
    FIELD_60           CHAR(100),
    PRINT_FLAG         CHAR(1),
    PRINT_DATE         DATE,
    constraint ATM_TRX_RECON_PK
        primary key (TRX_USR_SN1, TRX_USR, TRX_DATE, TRX_UNIT, ACCOUNT_NUMBER)
);

