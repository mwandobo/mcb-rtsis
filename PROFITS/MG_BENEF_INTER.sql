create table MG_BENEF_INTER
(
    FILE_NAME           CHAR(50) not null,
    SERIAL_NO           INTEGER  not null,
    BENEFICIARY_SN      SMALLINT,
    PRFT_CUST_ID        INTEGER,
    GUARANT_AMN         DECIMAL(15, 2),
    ROW_PROCESS_DATE    DATE,
    MAIN_BENEF_FLG      CHAR(1),
    ROW_STATUS          CHAR(1),
    BENEF_TYPE          CHAR(1),
    REF_PRFT_SYSTEM     CHAR(2),
    FILE_DETAIL_ID      CHAR(2),
    CUSTOMER_CODE       CHAR(20),
    PRFT_ACCOUNT_NUMBER CHAR(40),
    ACCOUNT_NO          CHAR(40),
    ROW_ERR_DESC        CHAR(80),
    CUST_ADDRESS_SN     SMALLINT,
    constraint IXU_MIG_002
        primary key (FILE_NAME, SERIAL_NO)
);

