create table COS_PARAMETERS
(
    ACTIVE_FLAG CHAR(1)     not null,
    PARAM_NAME  VARCHAR(20) not null,
    NUMBER_10_0 DECIMAL(10),
    NUMBER_15_2 DECIMAL(15, 2),
    DATE_VALUE  DATE,
    FLAG        CHAR(1),
    STRING_50   VARCHAR(50),
    DESCRIPTION VARCHAR(50),
    constraint IXU_CP_110
        primary key (ACTIVE_FLAG, PARAM_NAME)
);

