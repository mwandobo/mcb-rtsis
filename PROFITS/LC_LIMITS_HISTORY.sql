create table LC_LIMITS_HISTORY
(
    LC_ACCOUNT_NUMBER CHAR(40)    not null,
    HISTORY_SN        DECIMAL(10) not null,
    TRX_INTERNAL_SN   SMALLINT,
    TRX_CODE          INTEGER,
    ID_CURRENCY       INTEGER,
    TRX_UNIT          INTEGER,
    TRX_SN            INTEGER,
    LC_LIMIT_AMOUNT   DECIMAL(15, 2),
    LC_UTILIZED_LIMIT DECIMAL(15, 2),
    TRX_DATE          DATE,
    TMSTAMP           TIMESTAMP(6),
    TRX_USR           CHAR(8),
    constraint IXU_FX_010
        primary key (LC_ACCOUNT_NUMBER, HISTORY_SN)
);

