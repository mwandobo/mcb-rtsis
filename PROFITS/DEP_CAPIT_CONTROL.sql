create table DEP_CAPIT_CONTROL
(
    CUST_ID       INTEGER not null,
    TRX_DATE      DATE    not null,
    ID_CURRENCY   INTEGER not null,
    AMOUNT_USED   DECIMAL(15, 2),
    CURRENT_LIMIT DECIMAL(15, 2),
    AVAILABLE     DECIMAL(15, 2),
    constraint PK_CUSTOMER_CAP_LIMIT
        primary key (CUST_ID, ID_CURRENCY, TRX_DATE)
);

