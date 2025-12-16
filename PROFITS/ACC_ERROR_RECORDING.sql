create table ACC_ERROR_RECORDING
(
    TRX_DATE           DATE     not null,
    TRX_UNIT           INTEGER  not null,
    TRX_USR            CHAR(8)  not null,
    TRX_USR_SN         INTEGER  not null,
    TUN_INTERNAL_SN    SMALLINT not null,
    SUBSYSTEM          CHAR(2),
    ACC_ERROR_REASON   CHAR(80),
    ACC_ERROR_USER     CHAR(8),
    ACC_ERROR_TIMSTAMP TIMESTAMP(6),
    constraint IXU_ACCER_001
        primary key (TRX_DATE, TRX_UNIT, TRX_USR, TRX_USR_SN, TUN_INTERNAL_SN)
);

