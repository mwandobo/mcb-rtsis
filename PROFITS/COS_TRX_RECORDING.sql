create table COS_TRX_RECORDING
(
    TRX_DATE        DATE     not null,
    TRX_UNIT        INTEGER  not null,
    TRX_USR         CHAR(8)  not null,
    TRX_SN          INTEGER  not null,
    TUN_INTERNAL_SN SMALLINT not null,
    TRX_DEP_LNS_SN  INTEGER,
    TRX_TIMESTAMP   TIMESTAMP(6),
    TRX_CODE        INTEGER,
    TRX_CURRENCY    INTEGER,
    TRX_JUSTIFIC    INTEGER,
    U_USR_TOT_AMNT  DECIMAL(15, 2),
    TRN_TYPE        CHAR(1),
    REVERSED_FLAG   CHAR(1),
    TRX_PROFITS_ACC CHAR(40),
    SERVICE_PRODUCT INTEGER,
    MEMBER_ID       DECIMAL(10),
    constraint IXU_COS_022
        primary key (TRX_DATE, TRX_UNIT, TRX_USR, TRX_SN, TUN_INTERNAL_SN)
);

