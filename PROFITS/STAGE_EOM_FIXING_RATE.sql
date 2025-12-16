create table STAGE_EOM_FIXING_RATE
(
    EOM_DATE          DATE    not null,
    CURRENCY          INTEGER not null,
    RATE_DATE         DATE    not null,
    RATE              DECIMAL(12, 6),
    RATE_REVERSED     DECIMAL(12, 6),
    CURRENCY_ISO_CODE CHAR(3)
);

