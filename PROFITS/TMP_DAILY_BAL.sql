create table TMP_DAILY_BAL
(
    FK_CURRENCYID_CURR INTEGER,
    ACCOUNT_NUMBER     DECIMAL(11),
    RATE               DECIMAL(12, 6),
    TOT_BAL            DECIMAL(15, 2),
    ENTRY_AMOUNT       DECIMAL(15, 2),
    PREV_ACC_BALANCE   DECIMAL(15, 2),
    TOT_BAL_RATE       DECIMAL(15, 2),
    DATE_ID            DATE,
    HOLIDAY_IND        CHAR(1)
);

create unique index PKTMP_DAILY_BAL
    on TMP_DAILY_BAL (ACCOUNT_NUMBER, DATE_ID);

