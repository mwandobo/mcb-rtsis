create table STOP_PAYM_CHEQUES
(
    BANK_CODE          CHAR(3),
    CHEQUE_TYPE        CHAR(1),
    CHEQUE_NO          INTEGER,
    ACCOUNT_NO         CHAR(35),
    OTHER_BANK_UNIT    INTEGER,
    ISSUER_AFM         INTEGER,
    AMOUNT             DECIMAL(15, 2),
    TMSTAMP            TIMESTAMP(6),
    STOP_PAYM_DATE     DATE,
    ISSUE_DATE         DATE,
    RECORD_TYPE        CHAR(1),
    RECORD_SOURCE      CHAR(1),
    FILE_TYPE          CHAR(1),
    ENTRY_STATUS       CHAR(2),
    STOP_PAYMENT_DESC  CHAR(2),
    CURRENCY           CHAR(3),
    BUYER_BANK_CODE    CHAR(3),
    ISSUER_NAME        CHAR(70),
    SENT_FLAG          CHAR(1),
    LAST_CHEQUE_STATUS CHAR(1),
    SENT_DATE          DATE,
    B_DRAFT_FLAG       CHAR(1),
    IBAN               CHAR(27),
    BBAN               CHAR(23),
    BANK_ACC_NO        CHAR(16),
    PLAIN_ACC_NO       CHAR(16)
);

create unique index IXU_STO_000
    on STOP_PAYM_CHEQUES (BANK_CODE, CHEQUE_TYPE, CHEQUE_NO, ACCOUNT_NO);

