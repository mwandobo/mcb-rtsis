create table CALC_FLD_VALUES_HD
(
    FLD_VAL_SN         DECIMAL(10) not null
        constraint PK_CLCFLDVAL_HD
            primary key,
    FLD_VAL_DESC       VARCHAR(500),
    ENTRY_STATUS       CHAR(1),
    PRIMARY_FIELD_NAME CHAR(40),
    SECOND_FIELD_NAME  CHAR(40),
    TMSTAMP            TIMESTAMP(6),
    FKGH_SECOND_TYPE   CHAR(5),
    FKGH_PRIMARY_TYPE  CHAR(5)
);

