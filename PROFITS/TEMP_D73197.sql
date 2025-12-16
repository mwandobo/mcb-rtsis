create table TEMP_D73197
(
    ACCOUNT_NUMBER DECIMAL(11),
    RECORD_SN      DECIMAL(10) not null
        constraint PK_TEMP_D73197
            primary key,
    CARD_SN        DECIMAL(10),
    ENTRY_STS      CHAR(1)
);

