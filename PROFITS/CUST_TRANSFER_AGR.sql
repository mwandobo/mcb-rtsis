create table CUST_TRANSFER_AGR
(
    CUST_ID        INTEGER  not null,
    ACCOUNT_NUMBER CHAR(40) not null,
    PRFT_SYSTEM    SMALLINT not null,
    TARGET_UNIT    INTEGER  not null,
    ROW_STATUS     CHAR(1),
    ROW_ERR_DESC   CHAR(254),
    constraint IXU_CUSTRAGR_001
        primary key (PRFT_SYSTEM, ACCOUNT_NUMBER, CUST_ID)
);

