create table TEMP_TIME_DEPOS
(
    DURATION_VALUE  SMALLINT not null,
    DURATION_UNIT   CHAR(1)  not null,
    ID_CURRENCY     INTEGER  not null,
    UNIT_CODE       INTEGER  not null,
    COUNTER         INTEGER,
    RENEWAL_BALANCE DECIMAL(15, 2),
    constraint IXU_REP_219
        primary key (DURATION_VALUE, DURATION_UNIT, ID_CURRENCY, UNIT_CODE)
);

