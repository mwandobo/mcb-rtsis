create table CALC_FLD_VALUES_DT
(
    FLD_VAL_SERIAL_NUM DECIMAL(10) not null,
    PRIMARY_DIM_VAL    DECIMAL(18, 6),
    SECOND_DIM_VAL     DECIMAL(18, 6),
    FLD_VALUE          DECIMAL(18, 6),
    TMSTAMP            TIMESTAMP(6),
    FK_FLD_VAL_SN      DECIMAL(10) not null,
    FK_ID_CURRENCY     INTEGER,
    constraint PK_CLCFLDVAL_DT
        primary key (FK_FLD_VAL_SN, FLD_VAL_SERIAL_NUM)
);

