create table COLL_COUNTER
(
    UNIT         INTEGER not null,
    COUNTER_TYPE CHAR(5) not null,
    COLL_TYPE    INTEGER not null,
    CNTR         INTEGER,
    TMSTAMP      TIMESTAMP(6),
    constraint IXU_COL_034
        primary key (UNIT, COUNTER_TYPE, COLL_TYPE)
);

