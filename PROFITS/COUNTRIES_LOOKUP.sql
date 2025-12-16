create table COUNTRIES_LOOKUP
(
    COUNTRY_CODE VARCHAR(5)   not null,
    COUNTRY_NAME VARCHAR(100) not null
        primary key,
    CREATED_AT   TIMESTAMP(6) default CURRENT TIMESTAMP
);

