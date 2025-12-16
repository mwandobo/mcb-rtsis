create table MG_PARAM_PARAMETERS
(
    OLD_VALUE      CHAR(30) not null,
    PARAMETER_TYPE CHAR(5)  not null,
    NEW_VALUE      INTEGER  not null,
    NEW_TEXT_VALUE CHAR(10),
    constraint MGPARAMP
        primary key (PARAMETER_TYPE, OLD_VALUE)
);

