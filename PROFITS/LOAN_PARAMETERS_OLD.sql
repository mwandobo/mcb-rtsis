create table LOAN_PARAMETERS_OLD
(
    CURRENT_YEAR         SMALLINT not null
        constraint PK_LOAN_PARAMETERS
            primary key,
    ACC_AGREEM_WAIT_TIME SMALLINT,
    DOUBLE_YEAR_FLAG     CHAR(1),
    CATEG_PARAM_TYPE     CHAR(5)
);

