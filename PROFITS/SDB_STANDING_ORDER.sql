create table SDB_STANDING_ORDER
(
    TRX_DATE          DATE     not null,
    ACCOUNT_NUMBER    CHAR(40) not null,
    ACCOUNT_CD        SMALLINT,
    PRFT_SYSTEM       SMALLINT,
    SDB_UNIT          INTEGER,
    SDB_SN            INTEGER,
    SDB_PRODUCT       INTEGER,
    DEP_ACC_NUMBER    DECIMAL(11),
    DEP_AVAILABLE_AMN DECIMAL(15, 2),
    SDB_ASKED_AMN     DECIMAL(15, 2),
    SDB_COMMIS        DECIMAL(15, 2),
    SDB_EXPENSE       DECIMAL(15, 2),
    SDB_RENTAL        DECIMAL(15, 2),
    constraint PK_SDB_STORD
        primary key (ACCOUNT_NUMBER, TRX_DATE)
);

