create table CMS_B24_CASH_ATM
(
    FILE_SN            DECIMAL(10) not null,
    LINE_NO            DECIMAL(10) not null,
    PROGRAM_ID         CHAR(10),
    TRAN_DATE_YYYYMMDD INTEGER,
    TRAN_TIME_HHMISSTT INTEGER,
    TERMINAL_ID        CHAR(16),
    RECORD_TYPE        CHAR(2),
    CASH_REC_TYPE      CHAR(2),
    CASH_CODE          CHAR(2),
    CASH_AMNT          DECIMAL(15, 2),
    CASH_DATETIME      TIMESTAMP(6),
    CASH_UNIT_CODE     SMALLINT,
    COMPLETE_FILE_FLAG CHAR(1),
    FILE_REC_CNT       DECIMAL(10),
    TRX_DATE           DATE,
    CUTOVER_TIME       SMALLINT,
    TMSTAMP            TIMESTAMP(6),
    FULL_LINE          VARCHAR(1000),
    constraint PK_ATM_TERM_CASH
        primary key (LINE_NO, FILE_SN)
);

