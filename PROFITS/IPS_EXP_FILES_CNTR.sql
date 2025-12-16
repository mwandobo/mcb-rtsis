create table IPS_EXP_FILES_CNTR
(
    BIC_ADDRESS    VARCHAR(12) not null,
    ORDER_CURRENCY INTEGER     not null,
    TRX_DATE       DATE        not null,
    COUNTER        SMALLINT,
    constraint IPS_EXP_FILES_CNTR
        primary key (TRX_DATE, ORDER_CURRENCY, BIC_ADDRESS)
);

