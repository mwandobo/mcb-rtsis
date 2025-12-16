create table INVOICES_RECORDING
(
    TRX_SN             INTEGER not null,
    TRX_DATE           DATE    not null,
    TRX_USR            CHAR(8) not null,
    TRX_UNIT           INTEGER not null,
    ID_PRODUCT         INTEGER,
    ID_TRANSACT        INTEGER,
    ID_JUSTIFIC        INTEGER,
    INVOICE_TYPE       CHAR(1),
    INVOICE_ID         CHAR(20),
    CREDIT_NOTE_SN     INTEGER,
    ACCOUNT_NUMBER     CHAR(40),
    PRFT_SYSTEM        SMALLINT,
    LNS_OPEN_UNIT      INTEGER,
    LNS_TYPE           SMALLINT,
    LNS_SN             INTEGER,
    LG_ACC_SN          DECIMAL(13),
    LG_OPEN_UNIT       INTEGER,
    DEP_ACC_NUMBER     DECIMAL(11),
    DEP_OPEN_UNIT      INTEGER,
    REQUEST_TYPE       CHAR(1),
    REQUEST_SN         SMALLINT,
    REQUEST_LOAN_STS   CHAR(1),
    TMSTAMP            TIMESTAMP(6),
    REGISTRY_SN        INTEGER,
    REGISTRY_DESC      CHAR(200),
    VAT_RATE           DECIMAL(12, 6),
    INITIAL_INVOICE_ID CHAR(20),
    constraint INVOICES_REC_PK
        primary key (TRX_UNIT, TRX_USR, TRX_DATE, TRX_SN)
);

