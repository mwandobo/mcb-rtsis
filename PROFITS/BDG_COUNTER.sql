create table BDG_COUNTER
(
    TYPE     CHAR(1) not null
        constraint BDGCOUNT
            primary key,
    COUNTER  DECIMAL(11),
    FILLER   CHAR(250),
    TIMESTMP DATE
);

