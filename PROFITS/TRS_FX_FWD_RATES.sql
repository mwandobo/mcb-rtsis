create table TRS_FX_FWD_RATES
(
    CURRENCY        INTEGER not null,
    ACTIVATION_DATE DATE    not null,
    ACTIVATION_TIME TIME    not null,
    DURATION_DAYS   INTEGER not null,
    RATE            DECIMAL(16, 10),
    LST_UPD_USER    CHAR(8)
);

create unique index PK_FX_RATES
    on TRS_FX_FWD_RATES (CURRENCY, ACTIVATION_DATE, ACTIVATION_TIME, DURATION_DAYS);

