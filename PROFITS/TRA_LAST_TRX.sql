create table TRA_LAST_TRX
(
    TRA_RECORD_METHOD VARCHAR(80) not null
        constraint PK_TRA_LAST_TRX
            primary key,
    TRA_COUNTER       DECIMAL(15),
    TMSTAMP           TIMESTAMP(6),
    TRX_DATE          DATE
);

