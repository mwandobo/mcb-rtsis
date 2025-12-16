create table CURRENCY_RATES
(
    ACTIVATION_DATE    DATE    not null,
    FIXING_PRICE       DECIMAL(12, 6),
    STATUS             CHAR(1),
    FK_CURRENCYID_CURR INTEGER not null,
    constraint I0000329
        primary key (FK_CURRENCYID_CURR, ACTIVATION_DATE)
);

