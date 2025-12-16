create table MSG_PARAMETERS
(
    PARAMETER_NAME  VARCHAR(200) not null
        constraint IXM_PAR_000
            primary key,
    PARAMETER_VALUE VARCHAR(2000),
    DESCRIPTION     VARCHAR(2000)
);

