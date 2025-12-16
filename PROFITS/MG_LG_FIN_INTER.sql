create table MG_LG_FIN_INTER
(
    SERIAL_NO          INTEGER  not null,
    FILE_NAME          CHAR(50) not null,
    ROW_STATUS         SMALLINT,
    REQ_LOAN_SN        SMALLINT,
    UTF_NUM2           DECIMAL(15),
    UTF_NUM1           DECIMAL(15),
    RQ_ACR_NRM_INT_BAL DECIMAL(15, 2),
    RQ_EXP_BAL         DECIMAL(15, 2),
    RQ_COM_BAL         DECIMAL(15, 2),
    RQ_ACR_PNL_INT_BAL DECIMAL(15, 2),
    ROW_PROCESS_DATE   DATE,
    UTF_DATE1          DATE,
    UTF_DATE2          DATE,
    RQ_CREATION_DT     DATE,
    RQ_EXPIRE_DT       DATE,
    LST_ACR_CALC_DT    DATE,
    ROW_TMSTAMP        TIMESTAMP(6),
    REQUEST_TYPE       CHAR(1),
    REQUEST_LOAN_STS   CHAR(1),
    OLD_LG_NO          CHAR(40),
    UTF_TEXT2          CHAR(80),
    UTF_TEXT1          CHAR(80),
    ROW_ERR_DESC       CHAR(80),
    constraint IXU_MIG_042
        primary key (SERIAL_NO, FILE_NAME)
);

