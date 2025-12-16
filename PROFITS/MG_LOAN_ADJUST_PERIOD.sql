create table MG_LOAN_ADJUST_PERIOD
(
    SERIAL_NO          DECIMAL(6) not null
        constraint MG_LOAN_ADJUST_PERIOD
            primary key,
    OLD_LNS_NO         CHAR(40),
    ADJ_CUSTOMER_CODE  CHAR(20),
    ADJ_RECORD_SN      DECIMAL(3) not null,
    ADJ_REQ_INST_FROM  DECIMAL(4) not null,
    ADJ_REQ_INST_TO    DECIMAL(4) not null,
    ADJ_INSTALLMENTS   DECIMAL(3) not null,
    ADJ_DATE_FROM      DATE,
    ADJ_DATE_TO        DATE,
    ADJ_INTEREST_FLG   CHAR(1),
    ADJ_FIXED_INST_AMN DECIMAL(13, 2),
    ADJ_DATE           DATE,
    ADJ_COMMENTS       CHAR(30),
    ADJ_REASON         CHAR(30),
    INSTALL_FREQ       DECIMAL(3) not null,
    ROW_STATUS         SMALLINT,
    ROW_ERR_DESC       CHAR(80)
);

