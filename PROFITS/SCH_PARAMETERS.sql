create table SCH_PARAMETERS
(
    PARAMETER_NAME  VARCHAR(200)  not null
        constraint SCH_PARAMETERS_PK
            primary key,
    PARAMETER_VALUE VARCHAR(2000) not null,
    DESCRIPTION     VARCHAR(2000) not null
);

