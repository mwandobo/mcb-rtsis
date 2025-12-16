create table TF_SHIPMENT
(
    FK_LC_ACCOUNT_NUM CHAR(40) not null,
    SN                SMALLINT not null,
    SHIPMENT_DATE     DATE,
    SHIPMENT_DESCR    CHAR(20),
    constraint IXU_FX_035
        primary key (FK_LC_ACCOUNT_NUM, SN)
);

