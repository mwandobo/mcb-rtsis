create table BOT_90_STORHEADER
(
    STORHEADER_ID INTEGER generated always as identity
        constraint BOT_90_STORHEADER_ID_PK
            primary key,
    SOURCE        VARCHAR(128) not null,
    STORETO       DATE         not null,
    IDENTIFIER    VARCHAR(32)  not null,
    LOAN_CODE     VARCHAR(40)
);

