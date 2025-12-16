create table STAT_CUST_GRP
(
    CUST_GROUP        SMALLINT not null,
    CUSTOMER_CATEGORY INTEGER  not null,
    SN                INTEGER,
    constraint PKSTATCU
        primary key (CUST_GROUP, CUSTOMER_CATEGORY)
);

