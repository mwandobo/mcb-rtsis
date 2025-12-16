create table MG_CRED_LINE_INTER
(
    FILE_NAME         CHAR(50) not null,
    SERIAL_NO         INTEGER  not null,
    TRX_UNIT          INTEGER,
    GD_SERIAL_NUM     INTEGER,
    PRFT_CUST_ID      INTEGER,
    UTILISED_AMOUNT   DECIMAL(15, 2),
    CRLINE_AMOUNT     DECIMAL(15, 2),
    EXPIRE_DATE       DATE,
    ROW_PROCESS_DATE  DATE,
    ROW_STATUS        CHAR(1),
    GD_PARAMETER_TYPE CHAR(5),
    ACC_ID_CURRENCY   CHAR(5),
    CUSTOMER_CODE     CHAR(20),
    PRODUCT_GROUP     CHAR(30),
    ROW_ERR_DESC      CHAR(80),
    constraint IXU_MIG_018
        primary key (FILE_NAME, SERIAL_NO)
);

