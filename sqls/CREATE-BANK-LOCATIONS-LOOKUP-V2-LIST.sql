-- auto-generated definition
create table BANK_LOCATION_LOOKUP_V2
(
    REGION_CODE   VARCHAR(50)  null,
    REGION   VARCHAR(50)  null,
    DISTRICT_CODE VARCHAR(50)  null,
    DISTRICT VARCHAR(50)  null,
    WARD_CODE    VARCHAR(50) not null ,
    WARD     VARCHAR(50) not null,
    constraint PK_LC_LP_V2
        primary key (WARD, WARD_CODE)
);

