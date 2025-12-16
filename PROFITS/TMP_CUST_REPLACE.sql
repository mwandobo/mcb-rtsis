create table TMP_CUST_REPLACE
(
    NEW_CUSTOMER INTEGER not null,
    OLD_CUSTOMER INTEGER not null
        constraint IXU_CIU_061_TMP
            primary key,
    STATUS_FLAG  CHAR(1),
    CHG_DATE     DATE
);

