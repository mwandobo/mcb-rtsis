create table CIE_LOGGING
(
    CIE_PROFILE_ID    SMALLINT     not null,
    NET_TIMESTAMP     TIMESTAMP(6) not null,
    CUST_ID           INTEGER      not null,
    TUN_INTERNAL_SN   SMALLINT,
    TRX_UNIT          SMALLINT,
    NET_TRX_CODE      INTEGER,
    CIE_GATEWAY_UNIT  INTEGER,
    TRX_CODE          INTEGER,
    TRX_USR_SN        INTEGER,
    TRX_DATE          DATE,
    CIE_TIMESTAMP     TIMESTAMP(6),
    SRV_TIMESTAMP     TIMESTAMP(6),
    ERROR_INDICATOR   CHAR(2),
    TRX_USR           CHAR(8),
    ACTION_ENTRY_DESC CHAR(20),
    SESSION_ID        CHAR(32),
    constraint IXU_DEF_041
        primary key (CIE_PROFILE_ID, NET_TIMESTAMP, CUST_ID)
);

