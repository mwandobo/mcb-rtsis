create table BOT_60_INDADDRESSES
(
    INDADDRESSES_ID       INTEGER generated always as identity
        constraint BOT_60_INDADDRESSES_ID_PK
            primary key,
    FK_INDIVIDUAL         INTEGER
        constraint BOT_60_FKINDIVIDUAL
            references BOT_11_INDIVIDUAL,
    X__EMPLOYER           SMALLINT default 1,
    X__PERMANENTRESIDENCE SMALLINT default 1,
    X__PHYSICAL           SMALLINT default 1,
    X__POSTAL             SMALLINT default 1,
    REPORTING_DATE        DATE
);

