create table CHEQUE_PRINT_LOG
(
    PRINT_ID    TIMESTAMP(6) not null,
    SN          INTEGER      not null,
    TEMPLATE_ID INTEGER      not null,
    COMMAND     VARCHAR(255) not null,
    RESPONCE    VARCHAR(255),
    constraint PK_CHEQUE_PRINT_ID
        primary key (SN, PRINT_ID)
);

