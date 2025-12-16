create table DEPOSIT_ACCOUNT_RANGE
(
    PROGR_ID     SMALLINT not null
        constraint I0000263
            primary key,
    ACCOUNT_FROM DECIMAL(11),
    ACCOUNT_TO   DECIMAL(11),
    CNT          DECIMAL(15)
);

