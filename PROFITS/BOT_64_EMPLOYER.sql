create table BOT_64_EMPLOYER
(
    EMPLOYER_ID     INTEGER generated always as identity
        constraint BOT_64_EMPLOYER_ID_PK
            primary key,
    FK_INDADDRESSES INTEGER
        constraint BOT_64_FKINDADDRESSES
            references BOT_60_INDADDRESSES,
    CITYTOWNVILLAGE VARCHAR(64),
    COUNTRY         INTEGER not null,
    DISTRICT        INTEGER not null,
    HOUSENUMBER     VARCHAR(16),
    POBOXNUMBER     VARCHAR(16),
    REGION          INTEGER not null,
    STREETWARD      VARCHAR(64),
    REPORTING_DATE  DATE
);

