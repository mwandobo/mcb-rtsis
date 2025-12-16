create table CUSTOMER_PACK_IN_U
(
    CUST_ID        INTEGER not null,
    ID_PACKAGE     INTEGER not null,
    ID_PRODUCT     INTEGER not null,
    INSERTION_DT   DATE,
    PACKAGE_STATUS CHAR(1),
    SELLING_USR    CHAR(8),
    TOTAL_ACCS     INTEGER,
    constraint IXU_CIU_017
        primary key (ID_PRODUCT, ID_PACKAGE, CUST_ID)
);

