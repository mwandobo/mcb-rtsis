create table HUB_ERROR_LOG
(
    ID          INTEGER       not null
        constraint HUB_ERROR_LOG_PK
            primary key,
    TIMESTAMP   TIMESTAMP(6)  not null,
    SOURCE      VARCHAR(50)   not null,
    MESSAGE     VARCHAR(4000) not null,
    LOGICAL_KEY VARCHAR(200),
    TRACE       CLOB(1048576)
);

