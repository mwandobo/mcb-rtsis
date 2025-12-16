create table TEMP_D7A900
(
    SERIAL_NUMBER  DECIMAL(10) not null
        constraint PK_TEMP_D7A900
            primary key,
    ACCOUNT_NUMBER DECIMAL(11),
    ENTRY_STATUS   CHAR(1)
);

