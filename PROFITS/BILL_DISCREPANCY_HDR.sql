create table BILL_DISCREPANCY_HDR
(
    DISCR_SERIAL_NUM   INTEGER    not null,
    ENTRY_STATUS       VARCHAR(1),
    RECORD_COUNT       INTEGER,
    FK_USRCODE         VARCHAR(8) not null,
    FK_UNITCODE        INTEGER    not null,
    FK_CUSTOMERCUST_ID INTEGER    not null,
    DISCREPANCY_DATE   DATE,
    constraint PK_BILL_DISCR_HD
        primary key (FK_USRCODE, FK_UNITCODE, FK_CUSTOMERCUST_ID, DISCR_SERIAL_NUM)
);

