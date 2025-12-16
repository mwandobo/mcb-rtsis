create table ART_ENTITIES_ACCOUNTS
(
    SN           INTEGER generated always as identity,
    CURRENT_DATE DATE        not null,
    ENTITY_KEY   VARCHAR(30) not null,
    ACCOUNT_KEY  VARCHAR(40) not null,
    OWNERSHIP    CHAR(2),
    FILE_ACTION  CHAR(1) default 'F',
    primary key (SN, CURRENT_DATE, ENTITY_KEY, ACCOUNT_KEY)
);

