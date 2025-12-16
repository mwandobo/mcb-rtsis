create table BOT_3_STORCREDITCARD
(
    STORCREDITCARD_ID    INTEGER generated always as identity
        constraint BOT_3_STORCREDITCARD_ID_PK
            primary key,
    FK_COMMAND           INTEGER
        constraint BOT_3_FKCOMMAND
            references BOT_1_COMMAND,
    X__CREDITCARD        SMALLINT default 1,
    X__STORHEADER        SMALLINT default 1,
    FK_BOT_90_STORHEADER INTEGER
        constraint FK_BOT_3_BOT_90__
            references BOT_90_STORHEADER
);

