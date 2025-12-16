create table COLL_ISSUER_TOTALS
(
    ISSUER_CODE       INTEGER  not null,
    COLLATERAL_TYPE   INTEGER  not null,
    ENTRY_YEAR        SMALLINT not null,
    ENTRY_MONTH       SMALLINT not null,
    CHEQ_LACK_STS     CHAR(1)  not null,
    TOT_EST_VALUE_AMN DECIMAL(15, 2),
    ITEMS             INTEGER,
    constraint PK_ISSTOTA
        primary key (ISSUER_CODE, CHEQ_LACK_STS, ENTRY_YEAR, ENTRY_MONTH, COLLATERAL_TYPE)
);

