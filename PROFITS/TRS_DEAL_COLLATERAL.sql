create table TRS_DEAL_COLLATERAL
(
    SERIAL_NUMBER    DECIMAL(10)  not null,
    FK_DEAL_NO       INTEGER      not null,
    FK_TICKET_SN     INTEGER      not null,
    FK_TICKET_DATE   DATE         not null,
    FK_TICKET_REF_NO CHAR(16)     not null,
    MM_TRANS_TYPE    CHAR(1),
    BOND_ISIN        CHAR(15),
    BOND_CODE        CHAR(15),
    C_PRICE          DECIMAL(15, 8),
    C_ITEMS          DECIMAL(15),
    TMSTAMP          TIMESTAMP(6) not null,
    ENTRY_STATUS     CHAR(1)      not null,
    constraint PK_TRS_DEAL_COLLATERAL
        primary key (FK_DEAL_NO, FK_TICKET_DATE, FK_TICKET_SN, SERIAL_NUMBER)
);

