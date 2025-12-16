create table DICTIONARY_DETAIL
(
    ID             CHAR(11)     not null
        constraint DICTIONARY_DETAIL_UK1
            primary key,
    HELLENIC_BASE  VARCHAR(511) not null,
    AMERICAN_TRANS VARCHAR(511) not null,
    AVAILABLE      CHAR(1)      not null
);

