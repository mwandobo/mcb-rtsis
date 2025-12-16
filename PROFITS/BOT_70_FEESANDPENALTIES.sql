create table BOT_70_FEESANDPENALTIES
(
    FEESANDPENALTIES_ID INTEGER generated always as identity
        constraint BOT_70_FEESANDPENALTIES_ID_PK
            primary key,
    ADDITIONALFEESPAID  DECIMAL(19, 4),
    ADDITIONALFEESSUM   DECIMAL(19, 4),
    LOAN_CODE           VARCHAR(40)
);

