create table CUSTOMER_PACK_INFO
(
    CUST_ID        INTEGER not null,
    ID_PRODUCT     INTEGER not null,
    ID_PACKAGE     INTEGER not null,
    TOTAL_ACCS     INTEGER,
    INSERTION_DT   DATE,
    PACKAGE_STATUS CHAR(1),
    SELLING_USR    CHAR(8),
    constraint IXU_CUS_043
        primary key (CUST_ID, ID_PRODUCT, ID_PACKAGE)
);

