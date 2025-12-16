create table BOT_65_PERMRESIDENCE
(
    PERMRESIDENCE_ID INTEGER generated always as identity
        constraint BOT_65_PERMRESIDENCE_ID_PK
            primary key,
    FK_INDADDRESSES  INTEGER
        constraint BOT_65_FKINDADDRESSES
            references BOT_60_INDADDRESSES,
    CITYTOWNVILLAGE  VARCHAR(64),
    COUNTRY          INTEGER,
    DISTRICT         INTEGER,
    HOUSENUMBER      VARCHAR(16),
    POBOXNUMBER      VARCHAR(16),
    REGION           INTEGER,
    STREETWARD       VARCHAR(64),
    REPORTING_DATE   DATE
);

