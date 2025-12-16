create table DIAS_COUNTERS
(
    TYPE0   CHAR(5) not null
        constraint I0010701
            primary key,
    CNTR    DECIMAL(15),
    TMSTAMP TIMESTAMP(6)
);

