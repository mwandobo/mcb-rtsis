create table RPT_LIMITS
(
    ID          INTEGER not null
        constraint RPT_LIMITS_PK
            primary key,
    LIMIT_VALUE VARCHAR(10)
);

