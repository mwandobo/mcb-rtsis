create table ATM_IDENT_RESPONSE
(
    CARD_NUMBER CHAR(37) not null
        constraint ATM_IDENT_RESPONSE_PK
            primary key,
    CNTR        INTEGER,
    TMSTAMP     TIMESTAMP(6)
);

