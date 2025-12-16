create table DATE_COUNTER
(
    TRX_DATE     DATE    not null,
    COUNTER_TYPE CHAR(5) not null,
    CNTR         DECIMAL(12),
    TMSTAMP      TIMESTAMP(6),
    constraint PK_EXTTSK5
        primary key (COUNTER_TYPE, TRX_DATE)
);

