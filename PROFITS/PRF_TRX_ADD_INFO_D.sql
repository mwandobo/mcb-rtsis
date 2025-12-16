create table PRF_TRX_ADD_INFO_D
(
    TRX_DATE        DATE           not null,
    TRX_UNIT        INTEGER        not null,
    TRX_USER        CHAR(8)        not null,
    TRX_USR_SN      INTEGER        not null,
    TUN_INTERNAL_SN SMALLINT       not null,
    DB_CR           CHAR(1)        not null,
    ITEMS           DECIMAL(15)    not null,
    BALANCE         DECIMAL(15, 2) not null,
    FK_ID_CURRENCY  INTEGER        not null,
    FK_DENOMINATION DECIMAL(10, 2) not null,
    FK_NOTES_COINS  CHAR(1)        not null,
    PRFT_SYSTEM     INTEGER        not null,
    REVERSE_FLG     CHAR(1),
    constraint PK_PRF_TRX_D
        primary key (TRX_DATE, TRX_UNIT, TRX_USER, TRX_USR_SN, TUN_INTERNAL_SN, FK_ID_CURRENCY, FK_DENOMINATION,
                     FK_NOTES_COINS, DB_CR)
);

