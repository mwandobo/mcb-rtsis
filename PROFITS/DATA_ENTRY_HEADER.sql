create table DATA_ENTRY_HEADER
(
    TRX_DATE       DATE        not null,
    SERIAL_NO      DECIMAL(15) not null,
    SUPERVISE_UNIT INTEGER     not null,
    TRX_UNIT       INTEGER     not null,
    TOTAL_COUNT    DECIMAL(15),
    TOTAL_AMOUNT   DECIMAL(15, 2),
    TIMESTMP       DATE,
    TYPE           CHAR(1),
    DISPATCH_FLG   CHAR(1),
    STATUS         CHAR(1),
    SUPERVISE_USER CHAR(8),
    constraint IXU_REP_046
        primary key (TRX_DATE, SERIAL_NO, SUPERVISE_UNIT, TRX_UNIT)
);

