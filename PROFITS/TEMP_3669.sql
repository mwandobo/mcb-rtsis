create table TEMP_3669
(
    SERIAL_NUMBER  DECIMAL(10) not null,
    CUST_ID        INTEGER     not null,
    ACCOUNT_NUMBER CHAR(40)    not null,
    PRFT_SYSTEM    SMALLINT,
    CARD_SN        DECIMAL(10),
    FULL_CARD_NO   CHAR(40),
    constraint PK_TEMP_3669
        primary key (ACCOUNT_NUMBER, CUST_ID, SERIAL_NUMBER)
);

