create table UNIT_CATEGORY_PER_ROLE
(
    ID                 CHAR(8)      not null
        constraint PKUNITCATEGORYPER
            primary key,
    ENTRY_STATUS       CHAR(1)      not null,
    TMSTAMP            TIMESTAMP(6) not null,
    CUSTOMER_CONTROL   CHAR(1),
    PTJ_CHECK_OVERCOME CHAR(1),
    DESCRIPTION        CHAR(40),
    FK_SEC_ROLECODE    INTEGER      not null,
    FK_GENERIC_DETAFK  CHAR(5)      not null,
    FK_GENERIC_DETASER INTEGER      not null
);

