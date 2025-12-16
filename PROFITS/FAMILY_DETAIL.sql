create table FAMILY_DETAIL
(
    FK_PROVIDER_ID     DECIMAL(11) not null,
    FK_FAMILY_CODE     DECIMAL(11) not null,
    FK_GENERIC_DETAFK  CHAR(5)     not null,
    FK_GENERIC_DETASER INTEGER     not null,
    ACCOUNT_NO         CHAR(24)    not null,
    constraint IXU_CP_125
        primary key (FK_PROVIDER_ID, FK_FAMILY_CODE, FK_GENERIC_DETAFK, FK_GENERIC_DETASER, ACCOUNT_NO)
);

