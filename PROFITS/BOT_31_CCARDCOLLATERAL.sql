create table BOT_31_CCARDCOLLATERAL
(
    CCARDCOLLATERAL_ID   INTEGER generated always as identity
        constraint BOT_31_CCARDCOLLATERAL_ID_PK
            primary key,
    FK_CREDITCARD        INTEGER
        constraint BOT_31_FKCREDITCARD
            references BOT_8_CREDITCARD,
    KEY                  VARCHAR(32) not null,
    FK_BOT_43_COLLATERAL INTEGER
        constraint FK_BOT_31_BOT_43__
            references BOT_43_COLLATERAL
);

