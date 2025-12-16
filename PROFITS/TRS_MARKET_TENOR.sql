create table TRS_MARKET_TENOR
(
    ACTIVATION_DATE    DATE        not null,
    ACTIVATION_TIME    TIME        not null,
    ID_CURRENCY        INTEGER     not null,
    TENOR_DAYS         DECIMAL(10) not null,
    TENOR              SMALLINT    not null,
    TENOR_COUNT        DECIMAL(10) not null,
    YIELD              DECIMAL(12, 8),
    YIELD_INTERPOLATED CHAR(1)     not null,
    TENOR_UNIT_DAYS    INTEGER,
    BOND_DAYSBASE      SMALLINT,
    LST_UPD_USER       CHAR(8),
    TMSTAMP            TIMESTAMP(6),
    constraint PK_MARKET_TENOR
        primary key (ACTIVATION_DATE, ACTIVATION_TIME, ID_CURRENCY, TENOR_DAYS)
);

