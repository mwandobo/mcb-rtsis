create table BOT_11_INDIVIDUAL
(
    INDIVIDUAL_ID       INTEGER generated always as identity
        constraint BOT_11_INDIVIDUAL_ID_PK
            primary key,
    X__ENTREPRENEURDATA SMALLINT default 1,
    CUSTOMERCODE        VARCHAR(32) not null,
    REPORTING_DATE      DATE
);

