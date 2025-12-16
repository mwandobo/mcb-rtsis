create table MG_CUST_ADDR_INTER
(
    FILE_NAME        CHAR(50) not null,
    SERIAL_NO        INTEGER  not null,
    PRFT_ADDRESS_SN  SMALLINT,
    ADDR_SN          SMALLINT,
    PRFT_CUST_ID     INTEGER,
    ROW_PROCESS_DATE DATE,
    ROW_STATUS       CHAR(1),
    ADDRESS_TYPE     CHAR(1),
    COMM_ADDR_FLG    CHAR(1),
    LATIN_IND        CHAR(1),
    FILE_DETAIL_ID   CHAR(2),
    MAIL_BOX         CHAR(5),
    ZIP_CODE         CHAR(10),
    TELEPHONE        CHAR(15),
    FAX_NO           CHAR(15),
    CUSTOMER_CODE    CHAR(20),
    REGION           CHAR(20),
    COUNTRY          CHAR(30),
    CITY             CHAR(30),
    AREA             CHAR(30),
    ADDRESS_1        CHAR(40),
    ADDRESS_2        CHAR(40),
    ROW_ERR_DESC     CHAR(80),
    ENTRY_COMMENTS   CHAR(80),
    constraint IXU_MIG_040
        primary key (FILE_NAME, SERIAL_NO)
);

