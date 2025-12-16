create table CUST_TRAN_INFO_2
(
    TMSTAMP         TIMESTAMP(6),
    CUST_ID         INTEGER,
    SN              INTEGER,
    CATEGORY_CODE   CHAR(8),
    SERIAL_NUM      INTEGER,
    PREV_SERIAL_NUM INTEGER
);

create unique index IXU_CIU_62
    on CUST_TRAN_INFO_2 (TMSTAMP, CUST_ID, SN);

