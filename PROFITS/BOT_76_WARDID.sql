create table BOT_76_WARDID
(
    WARDID_ID          INTEGER generated always as identity
        constraint BOT_76_WARDID_ID_PK
            primary key,
    FK_IDENTIFICATIONS INTEGER
        constraint BOT_76_FKIDENTIFICATIONS
            references BOT_62_IDENTIFICATIONS,
    NUMBEROFWARDID     VARCHAR(16) not null,
    DATEOFEXPIRATION   DATE,
    DATEOFISSUANCE     DATE,
    ISSUANCELOCATION   VARCHAR(32),
    ISSUEDBY           VARCHAR(128),
    REPORTING_DATE     DATE
);

