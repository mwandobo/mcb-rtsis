create table HIST_MONTRAN_SO
(
    TP_SO_IDENTIFIER DECIMAL(10) not null,
    ACTIVATION_DATE  DATE        not null,
    TERRITORY_CODE   INTEGER,
    TAX_NO_TAX       INTEGER,
    TREASURY         INTEGER,
    BANK_ID          CHAR(9),
    BENEF_ACCOUNT    CHAR(27),
    GROUND           CHAR(254),
    INFORMATION      CHAR(254),
    constraint IXU_HIS_003
        primary key (TP_SO_IDENTIFIER, ACTIVATION_DATE)
);

