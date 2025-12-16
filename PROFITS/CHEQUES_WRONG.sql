create table CHEQUES_WRONG
(
    CW_SERIAL_NUMBER  DECIMAL(10)    not null
        constraint IXN_BIL_101
            primary key,
    BANK_ID           INTEGER        not null,
    REJECTED_CODE     INTEGER        not null,
    CHEQUE_AMOUNT     DECIMAL(15, 2) not null,
    CHEQUE_NUMBER     CHAR(20),
    CHQ_UNIT          INTEGER,
    TIMESTMP          TIMESTAMP(6),
    CHEQUE_STATUS     CHAR(1),
    CHEQUE_TYPE       CHAR(1),
    ENTRY_STATUS      CHAR(1)        not null,
    INSERT_DATE       DATE,
    SEND_DATE         DATE,
    CUST_ID           INTEGER,
    INS_UNIT          INTEGER,
    INS_USER          CHAR(8),
    CORR_ACC_NUMBER   CHAR(40),
    CORR_ACC_CD       SMALLINT,
    CORR_JUSTIFIC     INTEGER,
    INTERM_ACC_NUMBER CHAR(40),
    INTERM_ACC_CD     SMALLINT,
    INTERM_JUSTIFIC   INTEGER,
    DB_TUN_INT_SN     SMALLINT,
    DB_TRX_USR        CHAR(8),
    DB_TRX_USR_SN     INTEGER,
    DB_TRX_DATE       DATE,
    DB_TRX_UNIT       INTEGER,
    CR_TUN_INT_SN     SMALLINT,
    CR_TRX_USR        CHAR(8),
    CR_TRX_USR_SN     INTEGER,
    CR_TRX_DATE       DATE,
    CR_TRX_UNIT       INTEGER,
    CUST_CDIGIT       SMALLINT,
    FILE_NAME         CHAR(20)
);

