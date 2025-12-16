create table TRS_MARKET_PRICE
(
    ISIN             CHAR(15) not null,
    ACTIVATION_DATE  DATE     not null,
    ACTIVATION_TIME  TIME     not null,
    PRICE            DECIMAL(12, 8),
    LST_UPD_USER     CHAR(8),
    TENOR_YIELD_RATE DECIMAL(12, 8)
);

create unique index PK_MARKET_PRICE
    on TRS_MARKET_PRICE (ISIN, ACTIVATION_DATE, ACTIVATION_TIME);

