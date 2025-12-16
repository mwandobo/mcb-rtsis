create table COUNTERS
(
    COUNTER_TYPE CHAR(5),
    CNTR         DECIMAL(10),
    DEP_ACC_CNTR DECIMAL(11),
    TMSTAMP      TIMESTAMP(6)
);

create unique index IXU_COU_002
    on COUNTERS (COUNTER_TYPE);

