create table BOT_68_PASSPORT
(
    PASSPORT_ID        INTEGER generated always as identity
        constraint BOT_68_PASSPORT_ID_PK
            primary key,
    FK_IDENTIFICATIONS INTEGER
        constraint BOT_68_FKIDENTIFICATIONS
            references BOT_62_IDENTIFICATIONS,
    NUMBEROFPASSPORT   VARCHAR(16) not null,
    DATEOFEXPIRATION   DATE,
    DATEOFISSUANCE     DATE,
    ISSUANCELOCATION   VARCHAR(32),
    ISSUEDBY           VARCHAR(128),
    REPORTING_DATE     DATE
);

