create table CUST_REPLACE_ERR
(
    SYS_EXCEPTION VARCHAR(160) not null
        constraint IXU_CIU_071
            primary key,
    USR_EXCEPTION VARCHAR(160) not null
);

