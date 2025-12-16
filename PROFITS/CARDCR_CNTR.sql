create table CARDCR_CNTR
(
    CARDCR_COUNTER DECIMAL(12) generated always as identity
        constraint PK_CARDCR_CNTR
            primary key,
    TMSTAMP        TIMESTAMP(6)
);

