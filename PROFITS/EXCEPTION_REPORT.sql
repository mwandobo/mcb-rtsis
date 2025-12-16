create table EXCEPTION_REPORT
(
    PROGRAM_ID           CHAR(5),
    PROCESS_DATE         DATE,
    EXECUTION_SN         SMALLINT,
    INTERNAL_SN          INTEGER,
    C_DIGIT              SMALLINT,
    PRFT_SYSTEM          SMALLINT,
    ACC_CD               SMALLINT,
    ACC_TYPE             SMALLINT,
    ACC_UNIT             INTEGER,
    ID_JUSTIFIC          INTEGER,
    CUST_ID              INTEGER,
    ACC_SN               DECIMAL(15),
    AMOUNT               DECIMAL(15, 2),
    RM                   CHAR(8),
    EXIT_STATE           CHAR(80),
    LAP_RECORD_SN        DECIMAL(15),
    LAP_PROCESS_DATE     DATE,
    LAP_ACCOUNT_NUMBER_1 CHAR(40),
    LAP_ACCOUNT_NUMBER_2 CHAR(40)
);

create unique index IXU_EXC_000
    on EXCEPTION_REPORT (PROGRAM_ID, PROCESS_DATE, EXECUTION_SN, INTERNAL_SN);

