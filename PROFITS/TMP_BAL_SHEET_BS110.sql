create table TMP_BAL_SHEET_BS110
(
    FK_GLG_ACCOUNTACCO CHAR(21) not null,
    FK_CURRENCYID_CURR INTEGER  not null,
    FK_UNITCODE        INTEGER  not null,
    AMT                DECIMAL(15, 2),
    YTD                DECIMAL(15, 2),
    YEAR0              SMALLINT,
    PERIOD
    SMALLINT,
    LEVEL0 CHAR
(
    1
),
    QUARTER CHAR(2),
    constraint TMP_BAL_SHEET_BS110_PK
        primary key (FK_GLG_ACCOUNTACCO, FK_CURRENCYID_CURR, FK_UNITCODE)
    );

