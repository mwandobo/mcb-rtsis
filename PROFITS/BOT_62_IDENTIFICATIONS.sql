create table BOT_62_IDENTIFICATIONS
(
    IDENTIFICATIONS_ID             INTEGER generated always as identity
        constraint BOT_62_IDENTIFICATIONS_ID_PK
            primary key,
    FK_INDIVIDUAL                  INTEGER
        constraint BOT_62_FKINDIVIDUAL
            references BOT_11_INDIVIDUAL,
    X__DRIVINGLICENSE              SMALLINT default 1,
    X__NATIONALID                  SMALLINT default 1,
    X__PASSPORT                    SMALLINT default 1,
    X__PERMITFORPERMANENTRESIDENCE SMALLINT default 1,
    X__VOTERREGISTRATIONNUMBER     SMALLINT default 1,
    X__WARDID                      SMALLINT default 1,
    X__ZANZIBARID                  SMALLINT default 1,
    REPORTING_DATE                 DATE
);

