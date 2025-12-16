create table DP_TRXS_TO_CLEAR
(
    TRX_UNIT          INTEGER  not null,
    TRX_DATE          DATE     not null,
    TRX_USR           CHAR(8)  not null,
    TRX_USR_SN        INTEGER  not null,
    TUN_INTERNAL_SN   SMALLINT not null,
    SUPERVISOR_UNIT   INTEGER  not null,
    ACCOUNT_CD        SMALLINT,
    ANNOUNCE_UNIT     INTEGER,
    ACCOUNT_NUMBER    DECIMAL(11),
    TRX_AMOUNT        DECIMAL(15, 2),
    ORIG_TRX_DATE     DATE,
    TIMESTMP          DATE,
    REVERSED_FLG      CHAR(1),
    ENTRY_STATUS      CHAR(1),
    TYPE              CHAR(1),
    ANNOUNCE_USR      CHAR(8),
    ANNOUNCE_UNITNAME VARCHAR(40),
    constraint PKTRTOCL
        primary key (TRX_UNIT, TRX_DATE, TRX_USR, TRX_USR_SN, TUN_INTERNAL_SN, SUPERVISOR_UNIT)
);

