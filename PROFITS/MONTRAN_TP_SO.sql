create table MONTRAN_TP_SO
(
    TP_SO_IDENTIFIER DECIMAL(10) not null
        constraint IXU_MON_001
            primary key,
    TERRITORY_CODE   INTEGER,
    TAX_NO_TAX       INTEGER,
    TREASURY         INTEGER,
    BANK_ID          CHAR(9),
    BENEF_ACCOUNT    CHAR(27),
    GROUND           CHAR(254),
    INFORMATION      CHAR(254)
);

