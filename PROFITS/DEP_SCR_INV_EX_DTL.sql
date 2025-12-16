create table DEP_SCR_INV_EX_DTL
(
    ACCOUNT_NUMBER     DECIMAL(11) not null,
    EXTR_YEAR          SMALLINT    not null,
    EXTR_QTR           SMALLINT    not null,
    TRANS_SER_NUM      INTEGER     not null,
    ENTRY_SER_NUM      SMALLINT    not null,
    DEP_TRX_DATE       DATE,
    DEP_ID_JUSTIFIC    INTEGER,
    DEP_ENTRY_AMOUNT   DECIMAL(15, 2),
    DEP_DEBIT_CREDIT_F CHAR(1),
    constraint PK_DEP_SCR_INV_DTL
        primary key (ENTRY_SER_NUM, TRANS_SER_NUM, EXTR_QTR, EXTR_YEAR, ACCOUNT_NUMBER)
);

