create table TRX_RECOVERY
(
    REFERENCE_NUMBER   CHAR(40) not null,
    TRX_UNIT           INTEGER  not null,
    TRX_DATE           DATE     not null,
    TRX_USR            CHAR(8)  not null,
    TRX_USR_SN         INTEGER  not null,
    TUN_INTERNAL_SN    SMALLINT not null,
    GRP_SUBSCRIPT      SMALLINT not null,
    PRFT_SYSTEM        SMALLINT,
    ID_CHANNEL         INTEGER,
    THIRD_PARTY_REF_NO VARCHAR(40),
    CHANNEL_USER       VARCHAR(40),
    SOFT_ID            VARCHAR(10),
    TIMESTAMP          TIMESTAMP(6),
    WS_ID              VARCHAR(20),
    COMMAND            CHAR(80),
    CUST_ID            INTEGER,
    ACCOUNT_NUMBER     CHAR(40),
    constraint IXU_DEP_076
        primary key (GRP_SUBSCRIPT, TUN_INTERNAL_SN, TRX_USR_SN, TRX_USR, TRX_DATE, TRX_UNIT, REFERENCE_NUMBER)
);

create unique index IDX_A0B10002
    on TRX_RECOVERY (TRX_DATE, TRX_UNIT, TRX_USR, TRX_USR_SN, TUN_INTERNAL_SN);

create unique index IX_REFERENCE_NUMBER
    on TRX_RECOVERY (REFERENCE_NUMBER);

