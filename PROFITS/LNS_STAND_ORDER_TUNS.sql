create table LNS_STAND_ORDER_TUNS
(
    ACCOUNT_NUMBER    CHAR(40)     not null,
    PRFT_SYSTEM       SMALLINT     not null,
    ACCOUNT_CD        SMALLINT,
    SO_ACCOUNT_NUM    CHAR(40)     not null,
    SO_PRFT_SYSTEM    SMALLINT     not null,
    SO_ACCOUNT_CD     SMALLINT,
    LNS_TRX_TMSTAMP   TIMESTAMP(6) not null,
    LNS_TRX_DATE      DATE         not null,
    LNS_TRX_UNIT      INTEGER      not null,
    LNS_TRX_USR       CHAR(8)      not null,
    LNS_TRX_SN        INTEGER      not null,
    DEP_TRX_UNIT      INTEGER      not null,
    DEP_TRX_DATE      DATE         not null,
    DEP_TRX_USR       CHAR(8)      not null,
    DEP_TRX_SN        INTEGER      not null,
    ENTRY_COMMENTS    VARCHAR(40),
    LNS_AMOUNT        DECIMAL(15, 2),
    DEP_AMOUNT        DECIMAL(15, 2),
    TMSTAMP           TIMESTAMP(6),
    DEP_TRANS_SER_NUM INTEGER,
    DEP_ENTRY_SER_NUM INTEGER,
    constraint I0001062
        primary key (DEP_TRX_SN, DEP_TRX_USR, DEP_TRX_DATE, DEP_TRX_UNIT, LNS_TRX_SN, LNS_TRX_USR, LNS_TRX_UNIT,
                     LNS_TRX_DATE, LNS_TRX_TMSTAMP, SO_PRFT_SYSTEM, SO_ACCOUNT_NUM, PRFT_SYSTEM, ACCOUNT_NUMBER)
);

