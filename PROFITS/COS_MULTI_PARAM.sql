create table COS_MULTI_PARAM
(
    PARAM_ID    INTEGER     not null,
    PARAM_NAME  VARCHAR(20) not null,
    ACTIVE_FLAG CHAR(1),
    NUMBER_10_0 DECIMAL(10),
    NUMBER_15_2 DECIMAL(15, 2),
    DATE_VALUE  DATE,
    FLAG        CHAR(1),
    STRING_50   VARCHAR(50),
    DESCRIPTION VARCHAR(50),
    constraint PK_MULTI_PARAM
        primary key (PARAM_ID, PARAM_NAME)
);

