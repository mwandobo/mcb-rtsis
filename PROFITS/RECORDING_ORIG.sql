create table RECORDING_ORIG
(
    TRX_USER      CHAR(8)  not null,
    TRX_UNIT      INTEGER  not null,
    TRX_DATE      DATE     not null,
    TRX_USR_SN    INTEGER  not null,
    GRP_SUBSCRIPT SMALLINT not null,
    ORIGIN_TYPE   CHAR(1)  not null,
    CHARGE_CODE   INTEGER  not null,
    ORIGINID      CHAR(2)  not null,
    ACCOUNT_ID    CHAR(21) not null,
    constraint RECORDING_ORIG_PK
        primary key (ORIGINID, CHARGE_CODE, ORIGIN_TYPE, GRP_SUBSCRIPT, TRX_USR_SN, TRX_DATE, TRX_UNIT, TRX_USER)
);

