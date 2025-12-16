create table CUSTOMER_SYSTEM_U
(
    FK_CUSTOMERCUST_ID INTEGER      not null,
    FK_GENERIC_DETAFK  CHAR(5)      not null,
    FK_GENERIC_DETASER INTEGER      not null,
    TMSTAMP            TIMESTAMP(6) not null,
    constraint IXU_CIU_018
        primary key (FK_GENERIC_DETASER, FK_GENERIC_DETAFK, FK_CUSTOMERCUST_ID)
);

