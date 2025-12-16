create table BOT_49_MONTHLYINCOME
(
    MONTHLYINCOME_ID INTEGER generated always as identity
        constraint BOT_49_MONTHLYINCOME_ID_PK
            primary key,
    FK_PERSONALDATA  INTEGER
        constraint BOT_49_FKPERSONALDATA
            references BOT_63_PERSONALDATA,
    VALUE            DECIMAL(19, 4),
    CURRENCY         INTEGER,
    REPORTING_DATE   DATE
);

