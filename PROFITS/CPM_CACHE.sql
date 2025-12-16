create table CPM_CACHE
(
    VALUE_COLUMN    VARCHAR(100) not null,
    ORIGINAL_VALUE  VARCHAR(100) not null,
    PROCESSED_VALUE VARCHAR(100) not null,
    GREEKLISH_VALUE VARCHAR(100) not null,
    DB_NAME         VARCHAR(50)  not null,
    constraint ICPMCACHE
        primary key (VALUE_COLUMN, ORIGINAL_VALUE, DB_NAME)
);

