create table CHEQUES_TE_RETURN
(
    FILE_DATE          DATE     not null,
    CHEQUE_NUMBER      CHAR(20) not null,
    CHEQUE_UNIT        CHAR(4)  not null,
    CHEQUE_AMOUNT      DECIMAL(15, 2),
    FILE_NAME          CHAR(14),
    TMSTAMP            TIMESTAMP(6),
    FK_RECEIVE_BANK    INTEGER  not null,
    FK_CURRENCYID_CURR INTEGER  not null,
    FK_SEND_UNIT       INTEGER  not null,
    constraint I0010665
        primary key (FILE_DATE, FK_SEND_UNIT, FK_CURRENCYID_CURR, FK_RECEIVE_BANK, CHEQUE_NUMBER, CHEQUE_UNIT)
);

