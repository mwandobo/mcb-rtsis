create table TEMP_TIME_DEPOS2
(
    TRX_USR_SN        INTEGER not null,
    TRX_USR           CHAR(8) not null,
    TRX_DATE          DATE    not null,
    TRX_UNIT          INTEGER not null,
    I_ACCOUNT_C_DIGIT SMALLINT,
    TUN_INTERNAL_SN   SMALLINT,
    TRX_CODE          INTEGER,
    ID_CURRENCY       INTEGER,
    I_ID_JUSTIFIC     INTEGER,
    ID_PRODUCT        INTEGER,
    O_CR_INTER_RATE   DECIMAL(8, 4),
    I_ACCOUNT_NUMBER  DECIMAL(11),
    RENEWAL_BALANCE   DECIMAL(15, 2),
    AUTHORIZER2       CHAR(8),
    AUTHORIZER1       CHAR(8),
    ID_PRODUCT_DESC   CHAR(40),
    ID_JUSTIFIC_DESC  CHAR(40),
    constraint IXU_REP_094
        primary key (TRX_USR_SN, TRX_USR, TRX_DATE, TRX_UNIT)
);

