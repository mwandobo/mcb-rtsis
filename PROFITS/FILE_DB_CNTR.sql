create table FILE_DB_CNTR
(
    COUNTER_TYPE CHAR(8)     not null
        constraint PK_FILE_DB_CNTR
            primary key,
    TMSTAMP      TIMESTAMP(6),
    FILE_SN      DECIMAL(15) not null
);

