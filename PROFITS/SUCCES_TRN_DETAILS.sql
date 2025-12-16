create table SUCCES_TRN_DETAILS
(
    TRX_DATE        DATE     not null,
    TRX_UNIT        INTEGER  not null,
    TRX_USR         CHAR(8)  not null,
    TRX_USR_SN      INTEGER  not null,
    TUN_INTERNAL_SN SMALLINT not null,
    PRFT_SYSTEM     SMALLINT,
    WARNING_FLG     CHAR(1),
    ICOM_PAY_REF_NO CHAR(40),
    constraint IXU_DEP_075
        primary key (TRX_DATE, TRX_UNIT, TRX_USR, TRX_USR_SN, TUN_INTERNAL_SN)
);

