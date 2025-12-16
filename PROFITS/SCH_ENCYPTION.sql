create table SCH_ENCYPTION
(
    CIPHERMODE           SMALLINT default 1 not null,
    PADDINGMODE          SMALLINT default 2 not null,
    ENCRYPTIONALGORITHM  SMALLINT default 0 not null,
    IVTOFILE             SMALLINT default 0 not null,
    LABEL                VARCHAR(100)       not null
        constraint SCH_ENCYPTION_PK
            primary key,
    INITIALIZATIONVECTOR VARCHAR(4000),
    ENCRYPTIONKEY        VARCHAR(4000)      not null,
    ADDITIONAL           VARCHAR(200)
);

