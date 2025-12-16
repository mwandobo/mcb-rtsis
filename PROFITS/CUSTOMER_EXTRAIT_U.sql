create table CUSTOMER_EXTRAIT_U
(
    CUST_ID         INTEGER        not null,
    ACCOUNT_NUMBER  CHAR(40)       not null,
    TRX_UNIT        INTEGER        not null,
    TRX_DATE        DATE           not null,
    TRX_USER        CHAR(8)        not null,
    TRX_USR_SN      INTEGER        not null,
    TUN_INTERNAL_SN SMALLINT       not null,
    TMSTAMP         TIMESTAMP(6)   not null,
    ACCOUNT_CD      SMALLINT,
    TRX_CODE        INTEGER,
    JUSTIFIC_CODE   INTEGER,
    VALUE_DATE      DATE,
    TRX_COMMENTS    CHAR(40)       not null,
    TRX_AMOUNT      DECIMAL(15, 2),
    ACCOUNT_BALANCE DECIMAL(15, 2) not null,
    constraint IXU_CIU_016
        primary key (TMSTAMP, TUN_INTERNAL_SN, TRX_USR_SN, TRX_USER, TRX_DATE, TRX_UNIT, ACCOUNT_NUMBER, CUST_ID)
);

