create table HCUSTOMER_COL_PE_U
(
    ID_PRODUCT   INTEGER      not null,
    CUST_ID      INTEGER      not null,
    TMSTAMP      TIMESTAMP(6) not null,
    PERCENTAGE   DECIMAL(8, 4),
    ENTRY_STATUS CHAR(1),
    constraint IXU_CIU_040
        primary key (TMSTAMP, CUST_ID, ID_PRODUCT)
);

