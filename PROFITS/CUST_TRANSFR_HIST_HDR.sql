create table CUST_TRANSFR_HIST_HDR
(
    TRX_SN      INTEGER not null,
    TRX_USR     CHAR(8) not null,
    TRX_DATE    DATE    not null,
    TRX_UNIT    INTEGER not null,
    CUST_CD     SMALLINT,
    TRX_CODE    INTEGER,
    TARGET_UNIT INTEGER,
    CUST_ID     INTEGER,
    TIMESTAMP   TIMESTAMP(6),
    constraint IXU_CUS_034
        primary key (TRX_UNIT, TRX_DATE, TRX_USR, TRX_SN)
);

