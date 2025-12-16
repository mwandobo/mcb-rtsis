create table CP_OL_GROUP_DEF
(
    CPGROUP_CUST_ID   INTEGER     not null,
    CPGROUP_AGREEM_NO DECIMAL(10) not null,
    TMSTAMP           TIMESTAMP(6),
    ENTRY_STATUS      CHAR(1),
    constraint IXU_CP_121
        primary key (CPGROUP_CUST_ID, CPGROUP_AGREEM_NO)
);

