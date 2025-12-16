create table LOAN_SUBSIDY_HEADER
(
    ACC_UNIT           INTEGER not null,
    ACC_TYPE           INTEGER not null,
    ACC_SN             INTEGER not null,
    REQUEST_SN         INTEGER not null,
    REQUEST_TYPE       CHAR(1) not null,
    REQUEST_LOAN_STS   CHAR(1) not null,
    CREATION_DATE      DATE,
    CHECK_PAYMENT_FLAG CHAR(1),
    CHECK_PAYMENT_DATE DATE,
    TOTAL_AMOUNT       DECIMAL(15, 2),
    ACCRUAL_AMOUNT     DECIMAL(15, 2),
    SUBSIDY_AMOUNT     DECIMAL(15, 2),
    REMAINING_AMOUNT   DECIMAL(15, 2),
    ENTRY_STATUS       CHAR(1),
    TMSTAMP            TIMESTAMP(6),
    constraint PKLOANSUBSIDYHEADER
        primary key (ACC_UNIT, ACC_TYPE, ACC_SN, REQUEST_SN, REQUEST_TYPE, REQUEST_LOAN_STS)
);

