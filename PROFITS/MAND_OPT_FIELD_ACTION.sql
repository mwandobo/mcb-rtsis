create table MAND_OPT_FIELD_ACTION
(
    BANK_CODE      SMALLINT not null,
    WIND_CODE      INTEGER  not null,
    ATTR_CODE      INTEGER  not null,
    ACTION_CODE    CHAR(1)  not null,
    MANDATORY_FLAG CHAR(1),
    constraint IXU_DCD_045
        primary key (ACTION_CODE, ATTR_CODE, WIND_CODE, BANK_CODE)
);

