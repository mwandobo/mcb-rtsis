create table BOT_4_STORINSTALMENT
(
    STORINSTALMENT_ID    INTEGER generated always as identity
        constraint BOT_4_STORINSTALMENT_ID_PK
            primary key,
    FK_COMMAND           INTEGER
        constraint BOT_4_FKCOMMAND
            references BOT_1_COMMAND,
    X__INSTALMENT        SMALLINT default 1,
    X__STORHEADER        SMALLINT default 1,
    FK_BOT_90_STORHEADER INTEGER
        constraint FK_BOT_4_BOT_90__
            references BOT_90_STORHEADER
);

