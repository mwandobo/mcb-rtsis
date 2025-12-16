create table STAGE_W_EOM_FIXING_RATE
(
    EOM_DATE               DATE    not null,
    ACTIVATION_DATE        DATE,
    CURRENCY_ID            INTEGER not null,
    CURRENCY_CODE          CHAR(5),
    MULTIPLIER             INTEGER,
    RATE                   DECIMAL(12, 6),
    FIXING_TIMESTAMP       DATE,
    REVERSE_RATE           DECIMAL(12, 6),
    DOMESTIC_CURRENCY_FLAG VARCHAR(8),
    ACTIVATION_TIME        TIME,
    constraint STAGE_PK_W_EOM_FIXING_RATE
        primary key (EOM_DATE, CURRENCY_ID)
);

