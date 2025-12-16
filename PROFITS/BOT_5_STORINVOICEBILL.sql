create table BOT_5_STORINVOICEBILL
(
    STORINVOICEBILL_ID   INTEGER generated always as identity
        constraint BOT_5_STORINVOICEBILL_ID_PK
            primary key,
    FK_COMMAND           INTEGER
        constraint BOT_5_FKCOMMAND
            references BOT_1_COMMAND,
    X__INVOICEBILL       SMALLINT default 1,
    X__STORHEADER        SMALLINT default 1,
    FK_BOT_90_STORHEADER INTEGER
        constraint FK_BOT_5_BOT_90__
            references BOT_90_STORHEADER
);

