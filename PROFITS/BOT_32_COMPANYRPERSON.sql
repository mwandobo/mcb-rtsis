create table BOT_32_COMPANYRPERSON
(
    COMPANYRPERSON_ID       INTEGER generated always as identity
        constraint BOT_32_COMPANYRPERSON_ID_PK
            primary key,
    FK_COMPANY              INTEGER
        constraint BOT_32_FKCOMPANY
            references BOT_10_COMPANY,
    KEY                     VARCHAR(32) not null,
    FK_BOT_44_RELATEDPERSON INTEGER
        constraint FK_BOT_32_BOT_44__
            references BOT_44_RELATEDPERSON
);

