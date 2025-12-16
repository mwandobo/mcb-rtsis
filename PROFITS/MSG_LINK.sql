create table MSG_LINK
(
    SOURCE_ID   INTEGER      not null,
    SOURCE_TYPE INTEGER      not null,
    HEADER_ID   INTEGER      not null,
    VALUE_ID    INTEGER      not null,
    CREATED_BY  VARCHAR(20)  not null,
    CREATED_ON  TIMESTAMP(6) not null,
    constraint MSG_LINK_KEY
        primary key (HEADER_ID, VALUE_ID, SOURCE_ID, SOURCE_TYPE)
);

