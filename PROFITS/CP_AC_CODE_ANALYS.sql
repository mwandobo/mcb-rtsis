create table CP_AC_CODE_ANALYS
(
    PARAMETER_TYPE CHAR(5)  not null,
    PROFITS_VALUE  CHAR(50) not null,
    OUTPUT_VALUE   CHAR(1),
    constraint IXU_CP_120
        primary key (PARAMETER_TYPE, PROFITS_VALUE)
);

