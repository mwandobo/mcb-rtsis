-- auto-generated definition
create table CUSTOMER_TYPES_LOOKUP
(
    CUSTOMER_TYPE_CODE    CHAR(20)  not  null,
    CUSTOMER_TYPE     CHAR(100)     null,
    CREATED_AT  CHAR(20)   not  null,

    constraint CUSTOMER_TYPES_LOOKUP_PK
        primary key (CUSTOMER_TYPE_CODE)
);

