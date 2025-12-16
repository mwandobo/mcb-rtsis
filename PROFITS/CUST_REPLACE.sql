create table CUST_REPLACE
(
    NEW_CUSTOMER INTEGER not null,
    OLD_CUSTOMER INTEGER not null
        constraint IXU_CIU_061
            primary key,
    STATUS_FLAG  CHAR(1),
    CHG_DATE     DATE
);

