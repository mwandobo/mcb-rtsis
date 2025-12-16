create table DAILY_MM_REPORT
(
    DEAL_OPERATION   CHAR(5)  not null,
    BANK_CODE        SMALLINT not null,
    TRX_DATE         DATE     not null,
    SWAPS_FC         DECIMAL(15, 2),
    LOANS_DC         DECIMAL(15, 2),
    LOANS_FC         DECIMAL(15, 2),
    SWAPS_DC         DECIMAL(15, 2),
    COUNTRY_ISO_CODE CHAR(2),
    constraint IXU_REP_164
        primary key (DEAL_OPERATION, BANK_CODE, TRX_DATE)
);

