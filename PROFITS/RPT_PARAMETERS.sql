create table RPT_PARAMETERS
(
    PARAMETER_NAME  VARCHAR(200)  not null,
    PARAMETER_VALUE VARCHAR(2000) not null,
    DESCRIPTION     VARCHAR(2000) not null
);

create unique index RPT_PARAMETERS_PK
    on RPT_PARAMETERS (PARAMETER_NAME);

alter table RPT_PARAMETERS
    add constraint RPT_PARAMETERSS_PK
        primary key (PARAMETER_NAME);

