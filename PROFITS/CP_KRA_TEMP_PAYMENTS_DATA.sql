create table CP_KRA_TEMP_PAYMENTS_DATA
(
    DATA_KEY         VARCHAR(100) not null,
    SERIAL_NO        DECIMAL(10)  not null,
    ADVICE_ID        DECIMAL(10)  not null,
    KRA_PAYMENT_DATA VARCHAR(200),
    constraint PK_CP_KRA_TEMP_PAYMENTS_DATA
        primary key (SERIAL_NO, DATA_KEY)
);

