create table TEMP_DENOMINATION
(
    ID_CURRENCY      INTEGER        not null,
    DENOMINATION     DECIMAL(10, 2) not null,
    NOTES_COINS_TYPE CHAR(1)        not null,
    ITEMS            DECIMAL(15)    not null,
    BALANCE          DECIMAL(15, 2) not null,
    DB_CR            CHAR(1)        not null,
    USR              CHAR(8)        not null,
    TMSTAMP          TIMESTAMP(6)   not null,
    INTERNAL_SN      DECIMAL(10)    not null,
    TRX_DATE         DATE           not null,
    TRX_UNIT         INTEGER        not null,
    TRX_USER         CHAR(8)        not null,
    TRX_USR_SN       INTEGER        not null,
    TUN_INTERNAL_SN  SMALLINT       not null,
    constraint PK_TMP_DENOM
        primary key (USR, ID_CURRENCY, INTERNAL_SN, TMSTAMP)
);

create unique index SEC_TMP_DENOM
    on TEMP_DENOMINATION (TRX_DATE, TRX_UNIT, TRX_USER, TRX_USR_SN, TUN_INTERNAL_SN, ID_CURRENCY);

