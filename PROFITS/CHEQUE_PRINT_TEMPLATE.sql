create table CHEQUE_PRINT_TEMPLATE
(
    TEMPLATE_ID      INTEGER      not null
        constraint PK_CHEQUE_TEMPLATE
            primary key,
    DESCRIPTION      VARCHAR(200) not null,
    ADV_PAPER_SPEED  SMALLINT     not null,
    ADV_OUTPUT_SPEED SMALLINT     not null,
    ADV_PORT         VARCHAR(5)   not null
);

