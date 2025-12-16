create table CHEQUE_PRINT_PARAMETERS
(
    NAME        VARCHAR(15)  not null
        constraint PK_CHEQUE_PARAM
            primary key,
    VALUE       VARCHAR(200) not null,
    DESCRIPTION VARCHAR(200)
);

