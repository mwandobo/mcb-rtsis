create table CUSTOMER_CATEGOR_U
(
    FK_CUSTOMERCUST_ID INTEGER      not null,
    FK_CATEGORYCATEGOR CHAR(8)      not null,
    FK_GENERIC_DETAFK  CHAR(5),
    FK_GENERIC_DETASER INTEGER,
    TMSTAMP            TIMESTAMP(6) not null,
    constraint IXU_CIU_013
        primary key (FK_CATEGORYCATEGOR, FK_CUSTOMERCUST_ID)
);

