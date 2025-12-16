create table TEMP_D73100
(
    SERIAL_NUMBER  DECIMAL(10) not null
        constraint PK_TEMP_D73100
            primary key,
    ACCOUNT_NUMBER CHAR(40)    not null,
    RECORD_SN      DECIMAL(10) not null,
    ENTRY_STS      CHAR(1)
);

