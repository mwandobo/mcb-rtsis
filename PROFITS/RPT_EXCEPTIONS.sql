create table RPT_EXCEPTIONS
(
    ID         BIGINT        not null
        constraint RPT_EXCEPTIONS_PK
            primary key,
    TIME_STAMP TIMESTAMP(6)  not null,
    SOURCE     VARCHAR(100)  not null,
    EXCEPTION  VARCHAR(4000) not null,
    DATA       VARCHAR(4000)
);

