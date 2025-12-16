create table STOP_PAYM_CHEQUES_HIST
(
    BANK_CODE          CHAR(3)      not null,
    CHEQUE_TYPE        CHAR(1)      not null,
    OTHER_BANK_UNIT    INTEGER,
    ACCOUNT_NO         CHAR(35)     not null,
    CHEQUE_NO          INTEGER      not null,
    ISSUER_NAME        CHAR(70),
    ISSUER_AFM         INTEGER,
    ENTRY_STATUS       CHAR(2),
    STOP_PAYM_DATE     DATE,
    BUYER_BANK_CODE    CHAR(3),
    ISSUE_DATE         DATE,
    CURRENCY           CHAR(3),
    AMOUNT             DECIMAL(15, 2),
    RECORD_SOURCE      CHAR(1),
    TMSTAMP            TIMESTAMP(6),
    HIST_TIMESTAMP     TIMESTAMP(6) not null,
    FILE_TYPE          CHAR(1),
    STOP_PAYMENT_DESC  CHAR(2),
    RECORD_TYPE        CHAR(1),
    LAST_CHEQUE_STATUS CHAR(1),
    SENT_FLAG          CHAR(1),
    SENT_DATE          DATE,
    B_DRAFT_FLAG       CHAR(1),
    IBAN               CHAR(27),
    BBAN               CHAR(23),
    BANK_ACC_NO        CHAR(16),
    PLAIN_ACC_NO       CHAR(16)
);

create unique index PK_STOP_PAYM_HIST
    on STOP_PAYM_CHEQUES_HIST (HIST_TIMESTAMP, CHEQUE_NO, ACCOUNT_NO, CHEQUE_TYPE, BANK_CODE);

