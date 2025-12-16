create table ATM_CARD_USED
(
    LIM_TYPE       CHAR(1)  not null,
    CARD_NUMBER    CHAR(37) not null,
    ACCOUNT_NUMBER CHAR(40) not null,
    PRFT_SYSTEM    SMALLINT not null,
    TRX_DATE       DATE     not null,
    AMOUNT_USED    DECIMAL(15, 2),
    constraint PK_ATM_CARD_USED
        primary key (TRX_DATE, PRFT_SYSTEM, ACCOUNT_NUMBER, CARD_NUMBER, LIM_TYPE)
);

