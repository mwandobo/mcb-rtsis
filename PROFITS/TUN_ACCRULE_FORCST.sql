create table TUN_ACCRULE_FORCST
(
    AA_ID           SMALLINT not null,
    I_USR           CHAR(8)  not null,
    TRX_UNIT        INTEGER  not null,
    TRX_DATE        DATE     not null,
    TRX_USR         CHAR(8)  not null,
    TRX_SN          INTEGER  not null,
    TRX_INTERNAL_SN SMALLINT not null,
    GL_RULE_CODE    INTEGER,
    I_UNIT          INTEGER,
    ID_CURRENCY     INTEGER,
    DEBIT_AMOUNT    DECIMAL(15, 2),
    CREDIT_AMOUNT   DECIMAL(15, 2),
    ACCOUNT_BALL    DECIMAL(15, 2),
    I_DATE          DATE,
    GL_ACCOUNT      CHAR(21),
    constraint IXU_TUN_000
        primary key (AA_ID, I_USR, TRX_UNIT, TRX_DATE, TRX_USR, TRX_SN, TRX_INTERNAL_SN)
);

