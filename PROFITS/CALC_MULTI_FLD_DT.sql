create table CALC_MULTI_FLD_DT
(
    FK_MLT_FLDVAL_SN  DECIMAL(10) not null,
    MULTI_FLDVAL_SN   DECIMAL(10) not null,
    DESCRIPTION       VARCHAR(500),
    CRITERIA_FLD_NAME CHAR(40),
    FLD_VAL_FROM      DECIMAL(15, 4),
    FLD_VAL_TO        DECIMAL(15, 4),
    FK_FLD_VAL_SN     DECIMAL(10),
    ENTRY_STATUS      CHAR(1),
    TMSTAMP           TIMESTAMP(6),
    constraint PK_CALC_MULTI_DT
        primary key (MULTI_FLDVAL_SN, FK_MLT_FLDVAL_SN)
);

