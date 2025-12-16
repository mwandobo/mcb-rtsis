create table CHEQUES_DEPOSIT_DE
(
    DEPOSIT_AMOUNT     DECIMAL(15, 2),
    FK_CHEQUES_FOR_IDE DECIMAL(13) not null
        constraint PCOLLDEP
            primary key,
    FK_CURRENCYID_CURR INTEGER,
    ENTRY_STATUS       CHAR(1)     not null,
    VALUE_DATE         DATE,
    AVAILABILITY_DATE  DATE
);

