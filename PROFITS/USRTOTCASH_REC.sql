create table USRTOTCASH_REC
(
    TUN_INTERNAL_SN SMALLINT not null,
    TRX_USR_SN      INTEGER  not null,
    TRX_USR         CHAR(8)  not null,
    TRX_DATE        DATE     not null,
    TRX_UNIT        INTEGER  not null,
    PRFT_SYSTEM     SMALLINT,
    C_DIGIT         SMALLINT,
    ID_CURR_TARGET  INTEGER,
    ID_CURRENCY     INTEGER,
    TRX_JUSTIFIC    INTEGER,
    TRX_CODE        INTEGER,
    CHECK_NO        DECIMAL(10),
    ACCOUNT_NUMBER  DECIMAL(11),
    JOURNAL_DIFF    DECIMAL(15, 2),
    JOURNAL_DB      DECIMAL(15, 2),
    CASH_DIFF       DECIMAL(15, 2),
    CASH_DB         DECIMAL(15, 2),
    CASH_CR         DECIMAL(15, 2),
    NON_FINANCIAL   DECIMAL(15, 2),
    JOURNAL_CR      DECIMAL(15, 2),
    CONFIRMED       CHAR(1),
    constraint IXU_REP_200
        primary key (TUN_INTERNAL_SN, TRX_USR_SN, TRX_USR, TRX_DATE, TRX_UNIT)
);

