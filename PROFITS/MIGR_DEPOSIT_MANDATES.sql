create table MIGR_DEPOSIT_MANDATES
(
    SERIAL_NO      DECIMAL(6) not null
        constraint PK_DEPOSIT_MANDATES
            primary key,
    ACCOUNT_NUMBER CHAR(40),
    CUSTOMER_CODE  CHAR(20),
    SIGN_WITH_IDS  CHAR(80),
    ROW_STATUS     SMALLINT,
    ROW_ERR_DESC   CHAR(80)
);

