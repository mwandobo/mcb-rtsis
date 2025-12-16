create table CUSTOMER_COL_PER_U
(
    ID_PRODUCT   INTEGER not null,
    CUST_ID      INTEGER not null,
    PERCENTAGE   DECIMAL(8, 4),
    ENTRY_STATUS CHAR(1),
    constraint IXU_CIU_014
        primary key (CUST_ID, ID_PRODUCT)
);

