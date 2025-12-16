create table TRX_COUNT_PAYMENT
(
    FK_USRCODE  CHAR(8) not null,
    TRX_DATE    DATE    not null,
    ADV_COUNTER DECIMAL(8),
    TRX_COUNTER DECIMAL(8),
    USR_STATUS  CHAR(1),
    constraint IXU_TRX_300
        primary key (FK_USRCODE, TRX_DATE)
);

