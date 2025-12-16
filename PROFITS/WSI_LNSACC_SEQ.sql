create table WSI_LNSACC_SEQ
(
    COUNTER_TYPE CHAR(6) not null,
    UNIT         INTEGER not null,
    COUNTER_SEQ  INTEGER generated always as identity,
    TMSTAMP      TIMESTAMP(6),
    constraint PK_WSI_LNSACC_SEQ
        primary key (COUNTER_SEQ, UNIT, COUNTER_TYPE)
);

