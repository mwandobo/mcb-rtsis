create table DEP_FOR_VOUCHER
(
    TRX_UNIT        INTEGER not null,
    TRX_DATE        DATE    not null,
    TRX_USR         CHAR(8) not null,
    TRX_USR_SN      INTEGER not null,
    TRX_CODE        INTEGER,
    OTHER_CUST_ID   INTEGER,
    OTHER_ID_NO     CHAR(20),
    OTHER_FULL_NAME VARCHAR(90),
    TMSTAMP         TIMESTAMP(6),
    constraint PK_DEP_FOR_VOUCHER
        primary key (TRX_USR_SN, TRX_USR, TRX_DATE, TRX_UNIT)
);

comment on column DEP_FOR_VOUCHER.TRX_USR_SN is '   get_trx_count';

comment on column DEP_FOR_VOUCHER.OTHER_CUST_ID is 'DEPOSITOR''S ID NO';

comment on column DEP_FOR_VOUCHER.OTHER_ID_NO is 'DEPOSITOR''S ID NO';

comment on column DEP_FOR_VOUCHER.OTHER_FULL_NAME is 'DEPOSITOR''S FULL NAME';

