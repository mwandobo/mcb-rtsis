create table LOAN_SUBSIDY_DETAIL
(
    TMSTAMP                    TIMESTAMP(6) not null,
    ACC_UNIT                   INTEGER      not null,
    ACC_TYPE                   INTEGER      not null,
    ACC_SN                     INTEGER      not null,
    REQUEST_SN                 INTEGER      not null,
    REQUEST_TYPE               CHAR(1)      not null,
    REQUEST_LOAN_STS           CHAR(1)      not null,
    TRX_DATE                   DATE,
    TRX_UNIT                   INTEGER,
    TRX_USER                   CHAR(8),
    TRX_USER_SN                INTEGER,
    ID_SUBSIDY                 INTEGER,
    TRX_CODE                   INTEGER,
    DEBIT_CREDIT_FLG           CHAR(1),
    TRX_AMOUNT                 DECIMAL(15, 2),
    TRANSFER_FLG               CHAR(1),
    ENTRY_STATUS               CHAR(1),
    TRANSFER_REQUEST_SN        INTEGER,
    TRANSFER_REQUEST_TYPE      CHAR(1),
    TRANSFER_REQUEST_LOAN_STS  CHAR(1),
    TOTAL_AMOUNT               DECIMAL(15, 2),
    TRANSFER_REQUEST_CREATE_DT DATE,
    INITIAL_ACCRUAL_AMOUNT     DECIMAL(15, 2),
    RETURN_REQUEST_SN          INTEGER,
    RETURN_REQUEST_TYPE        CHAR(1),
    RETURN_REQUEST_LOAN_STS    CHAR(1),
    RETURN_AMOUNT              DECIMAL(15, 2),
    constraint PKLOANSUBSIDYDETAIL
        primary key (TMSTAMP, ACC_UNIT, ACC_TYPE, ACC_SN, REQUEST_SN, REQUEST_TYPE, REQUEST_LOAN_STS)
);

