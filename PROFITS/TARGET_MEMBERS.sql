create table TARGET_MEMBERS
(
    COUNTRY_CODE CHAR(2) not null,
    SERVICE_CODE CHAR(3) not null,
    COUNTRY_NAME VARCHAR(40),
    SERVICE_NAME VARCHAR(80),
    constraint IXU_FX_056
        primary key (COUNTRY_CODE, SERVICE_CODE)
);

