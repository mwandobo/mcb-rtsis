create table TRS_FWD_POSITION
(
    FK_POS_CURRENCY INTEGER not null,
    VALID_DATE      DATE    not null,
    SALES_FC        DECIMAL(15, 2),
    PURCHASE_DC     DECIMAL(15, 2),
    SALES_DC        DECIMAL(15, 2),
    PURCHASE_FC     DECIMAL(15, 2),
    constraint IXU_FX_038
        primary key (FK_POS_CURRENCY, VALID_DATE)
);

