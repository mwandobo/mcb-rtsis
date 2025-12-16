create table TEMP_13777
(
    SERIAL_NUMBER      DECIMAL(10) not null
        constraint TEMP_13777_PK
            primary key,
    ACCOUNT_NUMBER     DECIMAL(11),
    PRF_ACCOUNT_NUMBER CHAR(40)
);

