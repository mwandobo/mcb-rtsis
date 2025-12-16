create table BOT_67_POSTAL
(
    POSTAL_ID       INTEGER generated always as identity
        constraint BOT_67_POSTAL_ID_PK
            primary key,
    FK_INDADDRESSES INTEGER
        constraint BOT_67_FKINDADDRESSES
            references BOT_60_INDADDRESSES,
    CITYTOWNVILLAGE VARCHAR(64),
    COUNTRY         INTEGER,
    DISTRICT        INTEGER,
    HOUSENUMBER     VARCHAR(16),
    POBOXNUMBER     VARCHAR(16),
    REGION          INTEGER,
    STREETWARD      VARCHAR(64),
    REPORTING_DATE  DATE
);

