create table CUSTOMER_TYPES_LOOKUP
(
    CUSTOMER_TYPE_CODE INTEGER      not null
        primary key,
    CUSTOMER_TYPE      VARCHAR(100) not null,
    CREATED_AT         TIMESTAMP(6) default CURRENT TIMESTAMP
);

