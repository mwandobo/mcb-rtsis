create table REUTERS_PARAMETERS
(
    PARAMETER_TYPE CHAR(5)      not null,
    PARAMETER_SN   INTEGER      not null,
    ENTRY_STATUS   CHAR(1)      not null,
    REUTERS_VALUE  VARCHAR(50),
    PROFITS_VALUE  VARCHAR(50),
    DESCRIPTION    VARCHAR(100),
    TMSTMP         TIMESTAMP(6) not null,
    constraint PK_REUT_PARMS
        primary key (PARAMETER_TYPE, PARAMETER_SN)
);

