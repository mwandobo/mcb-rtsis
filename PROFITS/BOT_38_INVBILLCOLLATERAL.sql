create table BOT_38_INVBILLCOLLATERAL
(
    INVBILLCOLLATERAL_ID INTEGER generated always as identity
        constraint BOT_38_INVBILLCOLLATERAL_ID_PK
            primary key,
    FK_INVOICEBILL       INTEGER
        constraint BOT_38_FKINVOICEBILL
            references BOT_19_INVOICEBILL,
    KEY                  VARCHAR(32) not null,
    FK_BOT_43_COLLATERAL INTEGER
        constraint FK_BOT_38_BOT_43__
            references BOT_43_COLLATERAL
);

