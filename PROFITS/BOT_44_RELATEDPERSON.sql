create table BOT_44_RELATEDPERSON
(
    RELATEDPERSON_ID INTEGER generated always as identity
        constraint BOT_44_RELATEDPERSON_ID_PK
            primary key,
    CELLPHONE        VARCHAR(32),
    FULLNAMEOFPERSON VARCHAR(256) not null,
    RELATIONTYPE     INTEGER      not null
);

