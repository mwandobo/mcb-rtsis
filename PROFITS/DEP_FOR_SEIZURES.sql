create table DEP_FOR_SEIZURES
(
    SERIAL_NUMBER      DECIMAL(10) generated always as identity
        constraint PK_DEP_SEIZURES
            primary key,
    ACCOUNT_NUMBER     DECIMAL(11)    not null,
    CREDIT_AMOUNT      DECIMAL(15, 2) not null,
    TRX_UNIT           INTEGER        not null,
    TRX_DATE           DATE           not null,
    TRX_USR            CHAR(8)        not null,
    TRX_USR_SN         INTEGER        not null,
    TUN_INTERNAL_SN    SMALLINT       not null,
    AVAILABLE_BALANCE  DECIMAL(15, 2),
    MONTHLY_CREDITS    DECIMAL(15, 2),
    MONTHLY_DEBITS     DECIMAL(15, 2),
    ACCOUNT_STATUS     CHAR(1),
    TRANSACTION_STATUS CHAR(1)
);

