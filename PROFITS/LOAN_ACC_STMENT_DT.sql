create table LOAN_ACC_STMENT_DT
(
    BATCH_DATE         DATE           not null,
    ACCOUNT_NUMBER     CHAR(40)       not null,
    ACCOUNT_PRFT_SYS   SMALLINT       not null,
    SN                 INTEGER        not null,
    TRX_DATE           DATE,
    OUTSTANDING_BAL    DECIMAL(15, 2) not null,
    DEBIT_AMOUNT       DECIMAL(15, 2),
    CREDIT_AMOUNT      DECIMAL(15, 2),
    VALEUR_DT          DATE,
    REQUEST_DTL        CHAR(14),
    TRX_DESC           CHAR(40),
    JUSTIFICATION_DESC CHAR(40),
    OV_OUTSTANDING_BAL DECIMAL(15, 2),
    PRINTING_SN        INTEGER,
    TRX_CODE           INTEGER,
    constraint PK_LNS_ACC_STMENT_DT
        primary key (SN, ACCOUNT_PRFT_SYS, ACCOUNT_NUMBER, BATCH_DATE)
);

