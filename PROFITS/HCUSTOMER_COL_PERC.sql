create table HCUSTOMER_COL_PERC
(
    TMSTAMP      TIMESTAMP(6) not null,
    CUST_ID      INTEGER      not null,
    ID_PRODUCT   INTEGER      not null,
    PERCENTAGE   DECIMAL(8, 4),
    ENTRY_STATUS CHAR(1),
    constraint IXU_HCU_000
        primary key (TMSTAMP, CUST_ID, ID_PRODUCT)
);

