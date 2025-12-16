create table CASH_TILL
(
    UNIT               INTEGER,
    FK_CURRENCYID_CURR INTEGER,
    TILL_NO            INTEGER,
    CONSIGNMENT_AMOUNT DECIMAL(15, 2),
    OPENING_BALANCE    DECIMAL(15, 2),
    TRX_DATE           DATE,
    FK_USRCODE         CHAR(8)
);

create unique index IXU_CAS_000
    on CASH_TILL (UNIT, FK_CURRENCYID_CURR, TILL_NO);

