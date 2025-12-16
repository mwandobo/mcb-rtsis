create table TEMP_FD_VALEUR_BAL
(
    INIT_BALANCE_FLAG CHAR(1)     not null,
    VALUE_DATE        DATE        not null,
    INT_CALC_PERIOD   DECIMAL(11) not null,
    ACCOUNT_NUMBER    DECIMAL(11) not null,
    VALEUR_BALANCE    DECIMAL(15, 2),
    constraint IXU_REP_084
        primary key (INIT_BALANCE_FLAG, VALUE_DATE, INT_CALC_PERIOD, ACCOUNT_NUMBER)
);

