create table COMPOSITE_VOUCHER
(
    MASTER_VOUCHER INTEGER  not null,
    SN             SMALLINT not null,
    CHILD_VOUCHER  INTEGER,
    constraint IXU_COM_007
        primary key (MASTER_VOUCHER, SN)
);

