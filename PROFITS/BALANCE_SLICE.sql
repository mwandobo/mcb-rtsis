create table BALANCE_SLICE
(
    REPORT_ID     INTEGER,
    SERIAL_NUMBER SMALLINT,
    HIGH          DECIMAL(15, 2),
    LOW           DECIMAL(15, 2)
);

create unique index IXU_BAL_000
    on BALANCE_SLICE (REPORT_ID, SERIAL_NUMBER);

