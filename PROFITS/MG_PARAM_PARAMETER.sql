create table MG_PARAM_PARAMETER
(
    OLD_VALUE      CHAR(30) not null,
    PARAMETER_TYPE CHAR(5)  not null,
    NEW_VALUE      INTEGER,
    NEW_TEXT_VALUE CHAR(10),
    NEW_REL_VALUE  CHAR(13),
    constraint IXU_MIG_043
        primary key (OLD_VALUE, PARAMETER_TYPE)
);

create unique index SMGPARA1
    on MG_PARAM_PARAMETER (NEW_VALUE, PARAMETER_TYPE, OLD_VALUE);

