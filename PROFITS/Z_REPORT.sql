create table Z_REPORT
(
    ACCOUNT                   VARCHAR(100),
    OVERDUE_DAYS              INTEGER,
    EOM_DAY                   DATE,
    NET_INT_LESS_60           DECIMAL(15, 2),
    NET_INT_LESS_90           DECIMAL(15, 2),
    BOOK_BALANCE_LESS_60      DECIMAL(15, 2),
    BOOK_BALANCE_LESS_90      DECIMAL(15, 2),
    FLAG_01                   CHAR(1),
    FLAG_02                   CHAR(1),
    REPORTING_BALANCE_LESS_90 DECIMAL(15, 2)
);

