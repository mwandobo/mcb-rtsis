create table CHEQUES_POSITION
(
    VALID_DATE         DATE           not null,
    PREV_BALANCE       DECIMAL(15, 2) not null,
    INFLOW             DECIMAL(15, 2) not null,
    OUTFLOW            DECIMAL(15, 2) not null,
    FK_CURRENCYID_CURR INTEGER        not null,
    FK_UNITCODE        SMALLINT       not null,
    FK_USRCODE         CHAR(8)        not null,
    constraint PCHQPOS
        primary key (FK_CURRENCYID_CURR, FK_UNITCODE, FK_USRCODE, VALID_DATE)
);

