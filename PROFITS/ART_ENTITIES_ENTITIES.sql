create table ART_ENTITIES_ENTITIES
(
    SN                    INTEGER generated always as identity,
    CURRENT_DATE          DATE        not null,
    ENTITY_KEY            VARCHAR(30) not null,
    ASSOCIATED_PERSON_KEY VARCHAR(30) not null,
    RELATIONSHIP_CODE     CHAR(2)     not null,
    FILE_ACTION           CHAR(1) default 'F',
    primary key (SN, CURRENT_DATE, ENTITY_KEY, ASSOCIATED_PERSON_KEY, RELATIONSHIP_CODE)
);

