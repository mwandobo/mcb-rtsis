-- auto-generated definition
create table COUNTRIES_LOOKUP
(
    COUNTRY_NAME   VARCHAR(200) not null,
    COUNTRY_CODE   VARCHAR(50)  null,
    CREATED_AT VARCHAR(50)  null,

    constraint COUNTRIES_LOOKUP_pk
        primary key (COUNTRY_NAME)
);

