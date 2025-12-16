create table PARTY_SEL_DETAIL
(
    REGION         SMALLINT not null,
    DETAIL_SN      INTEGER  not null,
    EXCEPT_DETAIL  CHAR(1)  not null,
    DETAIL_TYPE    SMALLINT not null,
    FK_CURRENCY_ID INTEGER  not null,
    FK_CUSTOMER_ID INTEGER  not null,
    FKGH_PMNT_TYPE CHAR(5),
    FKGD_PMNT_SN   INTEGER,
    constraint PK_PARTY_DETAILS
        primary key (FK_CURRENCY_ID, FK_CUSTOMER_ID, DETAIL_SN, DETAIL_TYPE)
);

