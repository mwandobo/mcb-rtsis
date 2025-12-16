create table REPOS_DURATIONS
(
    PROFIT_DURATION    SMALLINT not null,
    PROFIT_DUR_UNIT    CHAR(1)  not null,
    MAX_DUR_DAYS       SMALLINT,
    MIN_DUR_DAYS       SMALLINT,
    PENALTY_PERCENTAGE DECIMAL(9, 6),
    constraint IXU_REP_038
        primary key (PROFIT_DURATION, PROFIT_DUR_UNIT)
);

