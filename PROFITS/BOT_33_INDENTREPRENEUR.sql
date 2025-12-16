create table BOT_33_INDENTREPRENEUR
(
    INDENTREPRENEUR_ID INTEGER generated always as identity
        constraint BOT_33_INDENTREPRENEUR_ID_PK
            primary key,
    FK_INDIVIDUAL      INTEGER
        constraint BOT_33_FKINDIVIDUAL
            references BOT_11_INDIVIDUAL,
    KEY                VARCHAR(32) not null
);

