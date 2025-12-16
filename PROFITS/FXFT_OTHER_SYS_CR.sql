create table FXFT_OTHER_SYS_CR
(
    ORDER_NUMBER      INTEGER,
    ACTION_FLAG       CHAR(1),
    FXFT_TRX_UNIT     INTEGER  not null,
    FXFT_TRX_DATE     DATE     not null,
    FXFT_TRX_USR      CHAR(8)  not null,
    FXFT_TRX_SN       INTEGER  not null,
    FXFT_TUN_INTE_SN  SMALLINT not null,
    FXFT_PRFT_SYSTEM  SMALLINT not null,
    TRX_UNIT          INTEGER  not null,
    TRX_DATE          DATE     not null,
    TRX_USR           CHAR(8)  not null,
    TRX_USR_SN        INTEGER  not null,
    TUN_INTERNAL_SN   SMALLINT not null,
    PRFT_SYSTEM       SMALLINT,
    U_USER_TOTALS_AMN DECIMAL(15, 2),
    ACCOUNT_NUMBER    DECIMAL(11),
    constraint PK_OTHERS_CR
        primary key (FXFT_TRX_DATE, FXFT_TRX_UNIT, FXFT_TRX_USR, FXFT_TRX_SN, FXFT_TUN_INTE_SN, TRX_USR_SN,
                     TUN_INTERNAL_SN)
);

