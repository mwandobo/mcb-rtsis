create table BOT_6_STORNONINSTALMENT
(
    STORNONINSTALMENT_ID INTEGER generated always as identity
        constraint BOT_6_STORNONINSTALMENT_ID_PK
            primary key,
    FK_COMMAND           INTEGER
        constraint BOT_6_FKCOMMAND
            references BOT_1_COMMAND,
    X__NONINSTALMENT     SMALLINT default 1,
    X__STORHEADER        SMALLINT default 1,
    FK_BOT_90_STORHEADER INTEGER
        constraint FK_BOT_6_BOT_90__
            references BOT_90_STORHEADER
);

