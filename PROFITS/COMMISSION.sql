create table COMMISSION
(
    ID_COMMISSION INTEGER not null
        constraint PCOMM
            primary key,
    DESCRIPTION   VARCHAR(40),
    SHORT_DESCR   CHAR(5),
    USAGE_COUNTER SMALLINT,
    STATUS        CHAR(1),
    TMSTAMP       TIMESTAMP(6),
    APPLY_COM     CHAR(1),
    ENTRY_STATUS  CHAR(1)
);

