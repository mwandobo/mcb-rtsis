create table BOP_TRX_RECORDING
(
    TRX_DATE       DATE     not null,
    TRX_UNIT       SMALLINT not null,
    TRX_USER       CHAR(8)  not null,
    TRX_USER_SN    INTEGER  not null,
    GRP_SUBSCRIPT  SMALLINT not null,
    TMSTAMP        TIMESTAMP(6),
    CNTRY_PAR_TYPE CHAR(5),
    CNTRY_SN       INTEGER,
    CNTRY_ISO_CODE CHAR(10),
    BOPBN_PAR_TYPE CHAR(5),
    BOPBN_SN       INTEGER,
    BOPTR_PAR_TYPE CHAR(5),
    BOPTR_SN       INTEGER,
    BOP_AMOUNT     DECIMAL(15, 2),
    REVERSED_FLG   CHAR(1),
    SUBSYSTEM      CHAR(2),
    NIMEXE         CHAR(20),
    constraint I0000699
        primary key (GRP_SUBSCRIPT, TRX_USER_SN, TRX_USER, TRX_UNIT, TRX_DATE)
);

