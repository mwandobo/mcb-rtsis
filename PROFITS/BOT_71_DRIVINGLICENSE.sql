create table BOT_71_DRIVINGLICENSE
(
    DRIVINGLICENSE_ID      INTEGER generated always as identity
        constraint BOT_71_DRIVINGLICENSE_ID_PK
            primary key,
    FK_IDENTIFICATIONS     INTEGER
        constraint BOT_71_FKIDENTIFICATIONS
            references BOT_62_IDENTIFICATIONS,
    NUMBEROFDRIVINGLICENSE VARCHAR(16) not null,
    DATEOFEXPIRATION       DATE,
    DATEOFISSUANCE         DATE,
    ISSUANCELOCATION       VARCHAR(32),
    ISSUEDBY               VARCHAR(128),
    REPORTING_DATE         DATE
);

