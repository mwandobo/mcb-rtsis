create table MG_SPECIAL_TRANS
(
    DEP_TRN_STATUS    SMALLINT,
    PRFT_ACC_C_DIGIT  SMALLINT,
    ACCOUNT_NO_TAUT   INTEGER,
    TRX_UNIT          INTEGER,
    ACC_TRN_SERIAL_NO INTEGER,
    SERIAL_NO         INTEGER,
    RATE2             DECIMAL(8, 4),
    RATE1             DECIMAL(8, 4),
    PRFT_DEP_ACC_NO   DECIMAL(11),
    AMOUNT3           DECIMAL(15, 2),
    AMOUNT1           DECIMAL(15, 2),
    AMOUNT2           DECIMAL(15, 2),
    DATE1             DATE,
    DEP_TRN_PROC_DT   DATE,
    DATE3             DATE,
    DATE2             DATE,
    DEP_TRN_TMSTAMP   TIMESTAMP(6),
    ROW_STATUS        CHAR(1),
    FILE_DETAIL_ID    CHAR(2),
    FILE_TYPE         CHAR(3),
    MG_ACCOUNT_TYPE   CHAR(5),
    ACCOUNT_NO        CHAR(40),
    COMMENTS          CHAR(40),
    FILE_NAME         CHAR(50),
    DEP_TRN_ERR_DESC  CHAR(80)
);

create unique index MIG00385
    on MG_SPECIAL_TRANS (FILE_NAME, SERIAL_NO);

create unique index SK1_MGSPC
    on MG_SPECIAL_TRANS (ACCOUNT_NO, ACC_TRN_SERIAL_NO, DATE3);

create unique index SK1_MGTRX
    on MG_SPECIAL_TRANS (ACCOUNT_NO, ACCOUNT_NO_TAUT, ACC_TRN_SERIAL_NO);

