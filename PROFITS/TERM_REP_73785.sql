create table TERM_REP_73785
(
    TRX_UNIT       INTEGER     not null,
    ID_PRODUCT     INTEGER     not null,
    CUST_ID        INTEGER     not null,
    ACCOUNT_NUMBER DECIMAL(11) not null,
    TRX_DATE       DATE        not null,
    ID_CURRENCY    INTEGER     not null,
    CODE           INTEGER     not null,
    constraint IXU_REP_220
        primary key (TRX_UNIT, ID_PRODUCT, CUST_ID, ACCOUNT_NUMBER, TRX_DATE, ID_CURRENCY, CODE)
);

