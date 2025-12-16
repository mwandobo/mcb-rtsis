create table WSI_PLBLACC_SEQ
(
    COUNTER_TYPE CHAR(6) not null,
    COUNTER_SEQ  INTEGER generated always as identity,
    TMSTAMP      TIMESTAMP(6),
    constraint PK_WSI_PLBLACC_SEQ
        primary key (COUNTER_SEQ, COUNTER_TYPE)
);

