create table BOT_35_INSTCOLLATERAL
(
    INSTCOLLATERAL_ID    INTEGER generated always as identity
        constraint BOT_35_INSTCOLLATERAL_ID_PK
            primary key,
    FK_INSTALMENT        INTEGER
        constraint BOT_35_FKINSTALMENT
            references BOT_17_INSTALMENT,
    KEY                  VARCHAR(32) not null,
    FK_BOT_43_COLLATERAL INTEGER
        constraint FK_BOT_35_BOT_43__
            references BOT_43_COLLATERAL,
    CUST_ID              VARCHAR(32),
    LOAN_CODE            VARCHAR(40),
    REPORTING_DATE       DATE
);

