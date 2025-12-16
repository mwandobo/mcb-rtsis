create table TEMP_MNT_RECORDING
(
    TRX_DATE       DATE     not null,
    TRX_UNIT       INTEGER  not null,
    TRX_USER       CHAR(8)  not null,
    TRX_USR_SN     INTEGER  not null,
    GRP_SUBSCRIPT  SMALLINT not null,
    ACCOUNTED_FLAG SMALLINT,
    ACCOUNTING_SN  DECIMAL(5),
    constraint IXU_TEMPMNT_000
        primary key (TRX_DATE, TRX_UNIT, TRX_USER, TRX_USR_SN, GRP_SUBSCRIPT)
);

create unique index IXN_TEMPMNT_001
    on TEMP_MNT_RECORDING (ACCOUNTING_SN);

