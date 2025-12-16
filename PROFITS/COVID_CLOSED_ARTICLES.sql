create table COVID_CLOSED_ARTICLES
(
    TRX_DATE         DATE        not null,
    CUST_ACCOUNT_OLD CHAR(40)    not null,
    CUST_ACCOUNT_NEW CHAR(40)    not null,
    AMOUNT           DECIMAL(18, 2),
    EXPIRY_DATE      DATE,
    OLD_ARTICLE_SN   DECIMAL(15) not null,
    CURRENCY         DECIMAL(5),
    constraint COVID_CLOSED_ARTICLESPK1
        primary key (TRX_DATE, CUST_ACCOUNT_OLD, CUST_ACCOUNT_NEW, OLD_ARTICLE_SN)
);

