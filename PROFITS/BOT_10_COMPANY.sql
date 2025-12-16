create table BOT_10_COMPANY
(
    COMPANY_ID       INTEGER generated always as identity
        constraint BOT_10_COMPANY_ID_PK
            primary key,
    X__RELATEDPERSON SMALLINT default 1,
    CUSTOMERCODE     VARCHAR(32) not null,
    REPORTING_DATE   DATE
);

