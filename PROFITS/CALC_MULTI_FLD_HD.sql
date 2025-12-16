create table CALC_MULTI_FLD_HD
(
    MULTI_FLD_VAL_SN   DECIMAL(10) not null
        constraint PK_CALC_MULTI_FLD_HD
            primary key,
    MULTI_FLD_VAL_DESC VARCHAR(500),
    ENTRY_STATUS       CHAR(1),
    TMSTAMP            TIMESTAMP(6)
);

