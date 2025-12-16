create table BOT_77_ZANZIBARID
(
    ZANZIBARID_ID      INTEGER generated always as identity
        constraint BOT_77_ZANZIBARID_ID_PK
            primary key,
    FK_IDENTIFICATIONS INTEGER
        constraint BOT_77_FKIDENTIFICATIONS
            references BOT_62_IDENTIFICATIONS,
    NUMBEROFZANZIBARID VARCHAR(16) not null,
    DATEOFEXPIRATION   DATE,
    DATEOFISSUANCE     DATE,
    ISSUANCELOCATION   VARCHAR(32),
    ISSUEDBY           VARCHAR(128),
    REPORTING_DATE     DATE
);

