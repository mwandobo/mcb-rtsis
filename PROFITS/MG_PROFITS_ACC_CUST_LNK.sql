create table MG_PROFITS_ACC_CUST_LNK
(
    SERIAL_NO         INTEGER  not null,
    FILE_NAME         CHAR(50) not null,
    OLD_LNS_NO        CHAR(40) not null,
    OLD_CUSTOMER_CODE CHAR(40) not null,
    NEW_CUST_ID       INTEGER,
    PRFT_SYSTEM       INTEGER,
    LINK_SN           INTEGER,
    ADDRESS_SN        INTEGER,
    RECIPIENT_FLAG    CHAR(1),
    CREATE_USER       CHAR(40),
    UPDATE_USER       CHAR(40),
    CREATE_UNIT       INTEGER,
    UPDATE_UNIT       INTEGER,
    CREATE_DATE       DATE,
    UPDATE_DATE       DATE,
    LINK_COMMENTS     VARCHAR(2048),
    LINK_REASON       INTEGER,
    ROW_STATUS        CHAR(1),
    ROW_ERR_DESCR     CHAR(40),
    primary key (SERIAL_NO, FILE_NAME, OLD_LNS_NO, OLD_CUSTOMER_CODE)
);

