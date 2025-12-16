create table BOT_68_BIRTHDATA
(
    BIRTHDATA_ID    INTEGER generated always as identity
        constraint BOT_68_BIRTHDATA_ID_PK
            primary key,
    FK_PERSONALDATA INTEGER
        constraint BOT_68_FKPERSONALDATA
            references BOT_63_PERSONALDATA,
    BIRTHDATE       DATE not null,
    COUNTRY         INTEGER,
    DISTRICT        INTEGER,
    REPORTING_DATE  DATE
);

