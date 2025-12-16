create table EXPENSE
(
    ID_EXPENSE    INTEGER not null
        constraint PEXP
            primary key,
    DESCRIPTION   VARCHAR(40),
    SHORT_DESCR   CHAR(5),
    USAGE_COUNTER SMALLINT,
    STATUS        CHAR(1),
    TMSTAMP       TIMESTAMP(6),
    APPLY_EXP     CHAR(1)
);

