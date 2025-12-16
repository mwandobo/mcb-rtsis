create table BOT_28_BUSINESS
(
    BUSINESS_ID         INTEGER generated always as identity
        constraint BOT_28_BUSINESS_ID_PK
            primary key,
    FK_ADDRESSESCOMPANY INTEGER
        constraint BOT_28_FKADDRESSESCOMPANY
            references BOT_12_ADDRESSESCOMPANY,
    CITYTOWNVILLAGE     VARCHAR(64),
    COUNTRY             INTEGER,
    DISTRICT            INTEGER,
    HOUSENUMBER         VARCHAR(16),
    POBOXNUMBER         VARCHAR(16),
    REGION              INTEGER,
    STREETWARD          VARCHAR(64),
    REPORTING_DATE      DATE
);

