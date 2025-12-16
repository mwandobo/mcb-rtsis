create table MSG_DYN_CATEGORY
(
    HEADER_ID    INTEGER      not null,
    HEADER_VALUE VARCHAR(40)  not null,
    VALUE_ID     INTEGER      not null,
    VALUE        VARCHAR(40)  not null,
    DESCRIPTION  VARCHAR(100),
    CREATED_BY   VARCHAR(20)  not null,
    CREATED_ON   TIMESTAMP(6) not null,
    UPDATED_BY   VARCHAR(20)  not null,
    UPDATED_ON   TIMESTAMP(6) not null,
    constraint MSG_DYN_CATEGORY_KEY
        primary key (HEADER_ID, VALUE_ID)
);

