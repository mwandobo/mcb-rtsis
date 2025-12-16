create table ETL_TRX_COUNTER
(
    ETL_COUNTER DECIMAL(12) generated always as identity
        constraint PK_ETL_TRX_CNTR
            primary key,
    TMSTAMP     TIMESTAMP(6)
);

