create table MG_CUST_CATEGORIES
(
    FILE_NAME        CHAR(50) not null,
    SERIAL_NO        INTEGER  not null,
    FILE_DETAIL_ID   CHAR(2),
    CUSTOMER_CODE    CHAR(20),
    CATEGORY_CODE    CHAR(8),
    CATEGORY_VALUE   CHAR(30),
    ROW_PROCESS_DATE DATE,
    ROW_STATUS       CHAR(1),
    ROW_ERR_DESC     CHAR(80),
    constraint IXU_MGCUSTCAT_001
        primary key (FILE_NAME, SERIAL_NO)
);

