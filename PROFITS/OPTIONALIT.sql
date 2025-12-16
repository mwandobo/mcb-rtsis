create table OPTIONALIT
(
    OPTIONALITY_NUMBER SMALLINT       not null,
    VARIABLE_NUMBER_CR SMALLINT       not null,
    VARIABLE_NAME_CRIT CHAR(16)       not null,
    LOGICAL_OPERATOR   CHAR(2)        not null,
    VALUE_TEXT         CHAR(30)       not null,
    VALUE_NUMBER       DECIMAL(15, 6) not null,
    FK_LINETIMESTAMP_I TIMESTAMP(6)   not null,
    constraint I0000607
        primary key (FK_LINETIMESTAMP_I, OPTIONALITY_NUMBER)
);

