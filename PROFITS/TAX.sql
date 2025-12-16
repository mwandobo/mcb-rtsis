create table TAX
(
    ID_TAX        INTEGER not null
        constraint PK_TAX
            primary key,
    APPLY_TAX     CHAR(1),
    USAGE_COUNTER SMALLINT,
    SHORT_DESCR   CHAR(5),
    TMSTAMP       TIMESTAMP(6),
    ENTRY_STATUS  CHAR(1),
    DESCRIPTION   VARCHAR(40)
);

