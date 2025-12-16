create table ATR_CARD_ACCOUNT
(
    CARD_NUMBER  CHAR(16) not null,
    PRFT_ACCOUNT CHAR(40) not null,
    TMSTAMP      TIMESTAMP(6),
    CARD_STATUS  CHAR(1),
    CUST_ID      INTEGER,
    constraint IXU_ATM_037
        primary key (CARD_NUMBER, PRFT_ACCOUNT)
);

