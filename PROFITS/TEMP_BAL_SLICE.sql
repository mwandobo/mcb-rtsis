create table TEMP_BAL_SLICE
(
    DURATION_VALUE  SMALLINT not null,
    DURATION_UNIT   CHAR(1)  not null,
    ID_CURRENCY     INTEGER  not null,
    UNIT_CODE       INTEGER  not null,
    SLICE_SN        SMALLINT not null,
    COUNTER         INTEGER,
    RENEWAL_BALANCE DECIMAL(15, 2),
    constraint IXU_REP_175
        primary key (DURATION_VALUE, DURATION_UNIT, ID_CURRENCY, UNIT_CODE, SLICE_SN)
);

