create table TMP_DAILY_BAL_MM
(
    FK_SOURCE_CURRENCY INTEGER,
    DEAL_NO            INTEGER,
    PREV_ACC_BALANCE   DECIMAL(15, 2),
    ENTRY_AMOUNT       DECIMAL(15, 2),
    RATE               DECIMAL(15, 2),
    TOT_BAL_RATE       DECIMAL(15, 2),
    TOT_BAL            DECIMAL(15, 2),
    DATE_ID            DATE,
    DEAL_DATE          DATE,
    MATURITY_DATE      DATE,
    HOLIDAY_IND        CHAR(1)
);

create unique index PKTMP_DAILY_BAL_MM
    on TMP_DAILY_BAL_MM (DEAL_NO, DATE_ID);

