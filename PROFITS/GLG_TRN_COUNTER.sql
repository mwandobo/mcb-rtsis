create table GLG_TRN_COUNTER
(
    COUNT_TYPE CHAR(2)   not null
        constraint I0000648
            primary key,
    COUNTER    DECIMAL(11),
    TIMESTMP   TIMESTAMP(6),
    FILLER     CHAR(250) not null,
    FILLER2    CHAR(250)
);

