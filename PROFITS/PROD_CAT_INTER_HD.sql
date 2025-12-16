create table PROD_CAT_INTER_HD
(
    ALL_INTER_FLG              CHAR(1) not null,
    FK_GEN_DET_FK_GENERIC_HEAD CHAR(5) not null,
    FK_GEN_DET_SERIAL_NUM      INTEGER not null,
    FK_HPROD_ID_PRODUCT        INTEGER not null,
    FK_HPROD_VALIDITY_DATE     DATE    not null,
    TMSTAMP                    TIMESTAMP(6),
    constraint PROD_CAT_INTER_HD_PK
        primary key (FK_GEN_DET_FK_GENERIC_HEAD, FK_GEN_DET_SERIAL_NUM, FK_HPROD_ID_PRODUCT, FK_HPROD_VALIDITY_DATE)
);

