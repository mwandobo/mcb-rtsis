create table LOAN_VALEUR_BAL
(
    VALUE_DATE        DATE    not null,
    INIT_BALANCE_FLAG CHAR(1) not null,
    constraint PK_LOAN_VALEUR_BAL
        primary key (INIT_BALANCE_FLAG, VALUE_DATE)
);

