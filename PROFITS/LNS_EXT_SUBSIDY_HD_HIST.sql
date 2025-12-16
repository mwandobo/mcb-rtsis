create table LNS_EXT_SUBSIDY_HD_HIST
(
    APPLICATION_NO      CHAR(40)     not null,
    DEBT_NUMBER         CHAR(40)     not null,
    DEBT_CODE           CHAR(40),
    ACCOUNT_NUMBER      CHAR(40)     not null,
    CUST_ID             INTEGER,
    AFM_NO              CHAR(20),
    APPLICATION_DT      DATE,
    OLD_ACC_NUMBER      CHAR(40),
    DEBT_TYPE           CHAR(1),
    SUBSIDY_PROGRAM     INTEGER,
    APPROVAL_DT         DATE,
    DEACTIVATION_DT     DATE,
    DEACTIVATION_CODE   INTEGER,
    DEACTIVATION_REASON CHAR(80),
    EXTERNAL_IBAN       CHAR(40),
    DEBIT_OPEN_ACCOUNT  CHAR(40),
    WRITEOFF_ACCOUNT    CHAR(40),
    EXPENSES_LOAN_ACC   CHAR(40),
    TRX_USER            CHAR(8),
    TRX_DATE            DATE,
    TRX_TMSTAMP         TIMESTAMP(6) not null,
    TRX_ACTION          CHAR(20),
    constraint PK_SUB_HIST
        primary key (APPLICATION_NO, DEBT_NUMBER, ACCOUNT_NUMBER, TRX_TMSTAMP)
);

