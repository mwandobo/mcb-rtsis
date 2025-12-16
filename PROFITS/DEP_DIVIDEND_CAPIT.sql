create table DEP_DIVIDEND_CAPIT
(
    ACCOUNT_NUMBER          DECIMAL(11) not null,
    TRX_DATE                DATE        not null,
    ENTRY_STATUS            CHAR(1),
    DIVIDENT_ACCOUNT        DECIMAL(11),
    BOSA_ACCOUNT            DECIMAL(11),
    DIVID_ON_SHARE_CAP      DECIMAL(15, 2),
    NET_DIVID_SHARE_CAPITAL DECIMAL(15, 2),
    NET_DIVIDENDS           DECIMAL(15, 2),
    MIN_SHARE_CAPITAL       DECIMAL(15, 2),
    RETENTION_PERC          DECIMAL(8, 4),
    DIVIDEND_CAPITAL        DECIMAL(15, 2),
    CAPITALIZATION_AMNT     DECIMAL(15, 2),
    ERROR_DESCRIPTION       CHAR(80),
    TMSTAMP                 TIMESTAMP(6),
    BOSA_GROSS_INTER        DECIMAL(15, 2),
    constraint PK_TOTAL_INT_TO_FOSA
        primary key (TRX_DATE, ACCOUNT_NUMBER)
);

