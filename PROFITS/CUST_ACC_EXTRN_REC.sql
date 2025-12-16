create table CUST_ACC_EXTRN_REC
(
    TRX_DATE         DATE         not null,
    TRX_UNIT         INTEGER      not null,
    TRX_USER         CHAR(8)      not null,
    TRX_USR_SN       INTEGER      not null,
    TUN_INTERNAL_SN  SMALLINT     not null,
    CUST_ID          INTEGER,
    ID_CHANNEL       INTEGER,
    ACCOUNT_NUMBER   CHAR(40),
    ACCOUNT_CD       SMALLINT,
    PRFT_SYSTEM      SMALLINT,
    REGISTRATION_ID  CHAR(40),
    EXPIRATION_DATE  DATE,
    STATUS           CHAR(1),
    TMSTAMP          TIMESTAMP(6) not null,
    LAST_UPDATE_USER CHAR(8),
    LAST_UPDATE_DATE DATE,
    GENERIC_BIT      CHAR(5),
    GENERIC_NUM      DECIMAL(15, 2),
    FK_CHANTYPE_HEAD CHAR(5),
    FK_CHANTYPE_DET  INTEGER,
    constraint PK_CUACCEXT_REC
        primary key (TRX_DATE, TRX_UNIT, TRX_USER, TRX_USR_SN, TUN_INTERNAL_SN, TMSTAMP)
);

