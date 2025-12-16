create table RECON_PARAMETERS
(
    PARAMETER_NAME        VARCHAR(200)  not null
        constraint RECON_PARAMETERS_PK
            primary key,
    PARAMETER_VALUE       VARCHAR(2000) not null,
    PARAMETER_DESCRIPTION VARCHAR(2000) not null
);

