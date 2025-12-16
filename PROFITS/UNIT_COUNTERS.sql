create table UNIT_COUNTERS
(
    FK_UNITCODE  INTEGER not null,
    COUNTER_TYPE CHAR(5) not null,
    CNTR         DECIMAL(10),
    TMSTAMP      TIMESTAMP(6),
    constraint IXU_DEF_104
        primary key (FK_UNITCODE, COUNTER_TYPE)
);

