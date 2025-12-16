create table BOT_73_NATIONALID
(
    NATIONALID_ID      INTEGER generated always as identity
        constraint BOT_73_NATIONALID_ID_PK
            primary key,
    FK_IDENTIFICATIONS INTEGER
        constraint BOT_73_FKIDENTIFICATIONS
            references BOT_62_IDENTIFICATIONS,
    NUMBEROFNATIONALID VARCHAR(20) not null,
    DATEOFEXPIRATION   DATE,
    DATEOFISSUANCE     DATE,
    ISSUANCELOCATION   VARCHAR(32),
    ISSUEDBY           VARCHAR(128),
    REPORTING_DATE     DATE
);

