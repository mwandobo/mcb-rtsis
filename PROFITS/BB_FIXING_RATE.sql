create table BB_FIXING_RATE
(
    RATE_DATE DATE    not null,
    CURRENCY  INTEGER not null,
    RATE      DECIMAL(12, 6),
    constraint IXU_DEF_111
        primary key (RATE_DATE, CURRENCY)
);

