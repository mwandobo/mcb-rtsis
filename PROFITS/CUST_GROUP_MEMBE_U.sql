create table CUST_GROUP_MEMBE_U
(
    FK_CUSTOMERCUST_ID INTEGER      not null,
    FK_CUST_GROUPGROUP INTEGER      not null,
    FK_GENERIC_DETAFK  CHAR(5),
    FK_GENERIC_DETASER INTEGER,
    ENTRY_STATUS       CHAR(1)      not null,
    TMSTAMP            TIMESTAMP(6) not null,
    ENTRY_COMMENTS     VARCHAR(30),
    constraint IXU_CIU_028
        primary key (FK_CUST_GROUPGROUP, FK_CUSTOMERCUST_ID)
);

