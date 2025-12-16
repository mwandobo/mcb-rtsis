create table IPS_UNDUE_PAYMENTS
(
    RECV_DATE         TIMESTAMP(6)   not null,
    SEND_DATE         TIMESTAMP(6)   not null,
    PROCESSED_TMSTAMP TIMESTAMP(6)   not null,
    IN_FILENAME       VARCHAR(50)    not null,
    OUT_FILENAME      VARCHAR(50),
    STATUS            INTEGER        not null,
    ORDER_CODE        VARCHAR(20),
    INSTRUCTION_ID    VARCHAR(35)    not null
        primary key,
    DEBTOR_IBAN       VARCHAR(34)    not null,
    DEBTOR_NAME       VARCHAR(50)    not null,
    CREDITOR_CODE     VARCHAR(5)     not null,
    CREDITOR_NAME     VARCHAR(70)    not null,
    CREDITOR_IBAN     VARCHAR(34)    not null,
    RF_CODE           VARCHAR(30)    not null,
    REQUESTED_AMOUNT  DECIMAL(15, 2) not null,
    SETTLEDATE        DATE           not null,
    DEBITED_AMOUNT    DECIMAL(15, 2) not null,
    COBEN_1           VARCHAR(70),
    COBEN_2           VARCHAR(70),
    COBEN_3           VARCHAR(70),
    COBEN_4           VARCHAR(70),
    COBEN_5           VARCHAR(70),
    REJ_CODE          CHAR(4),
    PROCESS_RESULTS   VARCHAR(255),
    FULL_LINE_UPO     VARCHAR(549),
    FULL_LINE_UPI     VARCHAR(549)
);

