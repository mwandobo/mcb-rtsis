create table TF_EXTRAIT
(
    TF_EXTRAIT_SN     INTEGER  not null,
    TF_NUMBER         CHAR(40) not null,
    PAYM_PRF_ACC_SYS  SMALLINT,
    DOC_LOT_SN        SMALLINT,
    PAYM_SN           SMALLINT,
    TRX_INTERNAL_SN   SMALLINT,
    SETTL_SN          SMALLINT,
    TF_PRODUCT        INTEGER,
    TRX_CODE          INTEGER,
    JUSTIFIC          INTEGER,
    TF_UNIT           INTEGER,
    TRX_UNIT          INTEGER,
    TRX_SN            INTEGER,
    PREV_AMN          DECIMAL(15, 2),
    ENTRY_AMN         DECIMAL(15, 2),
    TRX_DATE          DATE,
    PAYM_DATE         DATE,
    SETTL_DATE        DATE,
    TMSTAMP           TIMESTAMP(6),
    REVERSAL_FLAG     CHAR(1),
    DB_CR_FLAG        CHAR(1),
    TRX_USER          CHAR(8),
    PAYM_LC_PRF_ACC   CHAR(40),
    PAYM_PRF_ACC      CHAR(40),
    LC_ACCOUNT_NUMBER CHAR(40),
    COMMENTS          CHAR(70),
    constraint IXU_FX_033
        primary key (TF_EXTRAIT_SN, TF_NUMBER)
);

