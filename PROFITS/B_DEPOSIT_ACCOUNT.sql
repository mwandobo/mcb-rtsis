create table B_DEPOSIT_ACCOUNT
(
    ACCOUNT_NUMBER    DECIMAL(11) not null
        constraint IXU_DEF_036
            primary key,
    AVAILABLE_BALANCE DECIMAL(15, 2),
    BOOK_BALANCE      DECIMAL(15, 2),
    SAVED_DATE        DATE
);

