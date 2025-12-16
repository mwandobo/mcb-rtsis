create table SAI_ALGORITHM
(
    ALGORITHM_ID      SMALLINT not null,
    INTERNAL_SN       SMALLINT not null,
    DF_AFFECTED       CHAR(30),
    DF_START_POSITION SMALLINT,
    DF_LENGTH         SMALLINT,
    DF_OPERATOR       CHAR(2),
    FK_FUNCTION_ID    SMALLINT
        constraint FK_FUNCTION_ID
            references SAI_FUNCTION,
    PARAM_VALUE_1     VARCHAR(30),
    PARAM_VALUE_2     VARCHAR(30),
    PARAM_VALUE_3     VARCHAR(30),
    PARAM_VALUE_4     VARCHAR(30),
    PARAM_VALUE_5     VARCHAR(30),
    PARAM_VALUE_6     VARCHAR(30),
    constraint PK_SAI_ALGORITHM
        primary key (INTERNAL_SN, ALGORITHM_ID)
);

create unique index FK_FUNCTION_ID
    on SAI_ALGORITHM (FK_FUNCTION_ID);

