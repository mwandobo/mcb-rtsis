create table BKUP_FD_VALEUR_BALANCE
(
    FK_DEPOSIT_ACCOACC DECIMAL(11) not null,
    INT_CALC_PERIOD    DECIMAL(11) not null,
    VALUE_DATE         DATE        not null,
    INIT_BALANCE_FLAG  CHAR(1)     not null,
    VALEUR_BALANCE     DECIMAL(15, 2)
);

