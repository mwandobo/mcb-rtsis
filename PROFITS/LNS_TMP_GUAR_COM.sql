create table LNS_TMP_GUAR_COM
(
    TMSTAMP        TIMESTAMP(6) not null,
    TRX_DATE       DATE         not null,
    GRP_SUBSCRIPT  SMALLINT     not null,
    ACC_UNIT       INTEGER      not null,
    ACC_TYPE       SMALLINT     not null,
    ACC_SN         INTEGER      not null,
    DAYS0          SMALLINT,
    DB_PRODUCT_AMN DECIMAL(15, 2),
    GUAR_COM_PERC  DECIMAL(8, 4),
    DAYSBASE       SMALLINT,
    COMMISSION_AMN DECIMAL(15, 2),
    constraint IXU_LOA_101
        primary key (TMSTAMP, TRX_DATE, GRP_SUBSCRIPT, ACC_UNIT, ACC_TYPE, ACC_SN)
);

