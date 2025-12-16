create table MG_SPECIAL_MOVEMENT
(
    FILE_NAME            CHAR(50) not null,
    FILE_TYPE            CHAR(3),
    FILE_DETAIL_ID       CHAR(2),
    SERIAL_NO            INTEGER  not null,
    MG_ACCOUNT_TYPE      CHAR(5),
    ACCOUNT_NO_TAUT      INTEGER  not null,
    ACCOUNT_NO           CHAR(40) not null,
    ACC_TRN_SERIAL_NO    INTEGER,
    TRX_UNIT             INTEGER,
    DATE1                DATE,
    DATE2                DATE,
    DATE3                DATE,
    AMOUNT1              DECIMAL(15, 1),
    AMOUNT2              DECIMAL(15, 2),
    AMOUNT3              DECIMAL(15, 2),
    RATE                 DECIMAL(4, 4),
    COMMENTS             CHAR(40),
    PRFT_DEP_ACC_NO      DECIMAL(11),
    PRFT_ACC_C_DIGIT     SMALLINT,
    DEP_TRN_TMSTAMP      TIMESTAMP(6),
    DEP_TRN_STATUS       SMALLINT,
    DEP_TRN_PROCESS_DATE DATE,
    DEP_TRN_ERR_DESC     CHAR(80),
    ROW_STATUS           CHAR(1),
    constraint I0000400
        primary key (SERIAL_NO, FILE_NAME)
);

