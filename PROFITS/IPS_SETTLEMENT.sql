create table IPS_SETTLEMENT
(
    REC_TYPE          CHAR(2)     not null,
    EXCHANGE_DATE     DATE        not null,
    FILE_SERIAL_NO    CHAR(8)     not null,
    CURRENCY_CODE     CHAR(2)     not null,
    BANK_NO           CHAR(2)     not null,
    FULL_LINE         VARCHAR(200),
    RECS_COUNT_1      INTEGER,
    RECS_VALUE_1      DECIMAL(15, 2),
    RECS_COUNT_2      INTEGER,
    RECS_VALUE_2      DECIMAL(15, 2),
    RECS_COUNT_3      INTEGER,
    RECS_VALUE_3      DECIMAL(15, 2),
    TRX_DATE          DATE,
    INS_TMSTAMP       TIMESTAMP(6),
    PROCESSED_FLAG    CHAR(1),
    PROCESSED_TMSTAMP TIMESTAMP(6),
    PROCESSED_RESULT  VARCHAR(200),
    FILENAME          CHAR(50),
    FILE_ID           DECIMAL(10) not null,
    GROUP_ID          DECIMAL(10) not null,
    LINE_NO           INTEGER     not null,
    constraint IPS_SETTLEMENT_PK
        primary key (FILE_ID, LINE_NO, GROUP_ID)
);

