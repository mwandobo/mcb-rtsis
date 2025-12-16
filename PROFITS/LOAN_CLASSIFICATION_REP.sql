create table LOAN_CLASSIFICATION_REP
(
    PORTFOLIO_CLASS            SMALLINT,
    ACC_TYPE                   SMALLINT,
    PRFT_SYSTEM                SMALLINT,
    ACCOUNT_CD                 SMALLINT,
    TOTAL_NUM_OF_INST          SMALLINT,
    NUM_OF_INST_PAID           SMALLINT,
    NUMBER_OF_DAYS_OV          SMALLINT,
    FK_UNITCODE                INTEGER,
    ACC_SN                     INTEGER,
    DEBIT_AMOUNT               DECIMAL(15, 2),
    EST_INSURANCE_AMN          DECIMAL(15, 2),
    PREV_PROVISION             DECIMAL(15, 2),
    OUTSTAND_LOAN_IN_PRINCIPAL DECIMAL(15, 2),
    PROVISIONING_RATE          DECIMAL(15, 2),
    SECURITY_SAVINGS           DECIMAL(15, 2),
    CREDIT_AMOUNT              DECIMAL(15, 2),
    MATURITY_DATE              DATE,
    OPEN_DATE                  DATE,
    CLASS                      VARCHAR(40),
    ACCOUNT_NUMBER             VARCHAR(40),
    NAME_OF_BORROWER           VARCHAR(91)
);

