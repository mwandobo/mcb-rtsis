create table DYNAMIC_VOUCHER
(
    TMSTAMP         TIMESTAMP(6) not null,
    TRX_DATE        DATE         not null,
    TRX_UNIT        INTEGER      not null,
    TRX_USER        CHAR(8)      not null,
    TRX_USR_SN      INTEGER      not null,
    GRP_SUBSCRIPT   SMALLINT     not null,
    VOUCHER_DETAILS VARCHAR(2048),
    constraint PK_DYN_VOUCHER
        primary key (GRP_SUBSCRIPT, TRX_USR_SN, TMSTAMP, TRX_UNIT, TRX_DATE, TRX_USER)
);

