create table BOT_40_NONINSTCOLLATERAL
(
    NONINSTCOLLATERAL_ID INTEGER generated always as identity
        constraint BOT_40_NONINSTCOLLATERAL_ID_PK
            primary key,
    FK_NONINSTALMENT     INTEGER
        constraint BOT_40_FKNONINSTALMENT
            references BOT_21_NONINSTALMENT,
    KEY                  VARCHAR(32) not null,
    FK_BOT_43_COLLATERAL INTEGER
        constraint FK_BOT_40_BOT_43__
            references BOT_43_COLLATERAL
);

