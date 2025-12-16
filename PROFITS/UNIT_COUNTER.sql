create table UNIT_COUNTER
(
    COUNTER_TYPE CHAR(6),
    UNIT         INTEGER,
    CNTR         INTEGER,
    TMSTAMP      TIMESTAMP(6)
);

create unique index IXU_UNI_002
    on UNIT_COUNTER (COUNTER_TYPE, UNIT);

