create table USR_CASH_LIMIT
(
    BALANCE_UP_LIMIT  DECIMAL(15, 2) not null,
    BALANCE_LOW_LIMIT DECIMAL(15, 2) not null,
    FK_ID_CURRENCY    INTEGER        not null,
    FK_USR_SEC_ROLE   CHAR(8)        not null,
    constraint PK_USR_CASH_LIMIT
        primary key (FK_USR_SEC_ROLE, FK_ID_CURRENCY)
);

