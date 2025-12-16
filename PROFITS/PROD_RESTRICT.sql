create table PROD_RESTRICT
(
    VALIDITY_DATE      DATE,
    ID_PRODUCT         INTEGER,
    ENTRY_STATUS       CHAR(1) not null,
    TMSTAMP            TIMESTAMP(6),
    FK_PRODUCTID_PRODU INTEGER not null,
    FK_RESTR_TYPEID_RE INTEGER not null,
    constraint PKPRODRESTRICT
        primary key (FK_RESTR_TYPEID_RE, FK_PRODUCTID_PRODU)
);

