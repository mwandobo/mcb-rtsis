create table CP_ATM_REFUND_LOAD
(
    LOAD_DATE              DATE         not null,
    FILE_ID                TIMESTAMP(6) not null,
    LINE_NO                INTEGER      not null,
    RECORD_TYPE            CHAR(2),
    VOUCHER_TYPE_CODE      CHAR(2),
    CURRENCY_CODE          CHAR(2),
    AMOUNT                 DECIMAL(15, 2),
    DEBIT_CLEARING_CENTER  CHAR(2),
    CREDIT_CLEARING_CENTER CHAR(2),
    REF_NO                 CHAR(15),
    FULL_LINE              VARCHAR(500),
    TMSTAMP                TIMESTAMP(6),
    constraint CP_ATM_REFUND_PK
        primary key (FILE_ID, LINE_NO, LOAD_DATE)
);

