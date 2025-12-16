create table CPM_MULTI_RECORDING
(
    TRX_DATE           DATE     not null,
    TRX_UNIT           INTEGER  not null,
    TRX_USER           CHAR(8)  not null,
    TRX_USR_SN         INTEGER  not null,
    TRX_INTERNAL_SN    SMALLINT not null,
    TRX_MULTI_SN       SMALLINT not null,
    HEADER_SN          DECIMAL(15),
    DETAIL_SN          DECIMAL(15),
    PROCESS_SN         DECIMAL(15),
    TRANSACTION_SN     DECIMAL(15),
    TP_TRX_CODE        INTEGER,
    TP_TUN_TMSTAMP     TIMESTAMP(6),
    TP_TUN_DATE        DATE,
    TP_TUN_UNIT        INTEGER,
    TP_TUN_USR         CHAR(8),
    TP_TUN_SN          INTEGER,
    TP_TUN_INTERNAL_SN SMALLINT,
    TMSTAMP            TIMESTAMP(6),
    TRX_REVERSED       CHAR(1),
    TP_AMOUNT          DECIMAL(18, 2),
    constraint I0001046
        primary key (TRX_DATE, TRX_UNIT, TRX_USER, TRX_USR_SN, TRX_INTERNAL_SN, TRX_MULTI_SN)
);

