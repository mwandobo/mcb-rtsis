create table TEMP_REP_73235
(
    UNIT_CODE    INTEGER not null
        constraint IXU_REP_186
            primary key,
    TOTAL_AMOUNT DECIMAL(15, 2),
    UNIT_NAME    CHAR(40)
);

