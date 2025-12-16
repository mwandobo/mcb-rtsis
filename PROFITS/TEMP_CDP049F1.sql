create table TEMP_CDP049F1
(
    RECONCILIATION_ID       SMALLINT,
    UNIT_CODE               SMALLINT,
    CHEQUE_NUMBER           INTEGER,
    CHEQUE_AMOUNT           DECIMAL(11, 2),
    PUBLICATION_DATE        DATE,
    TRANSACTION_DATE        DATE,
    DHSSE_INCOMING_CHQ_FLAG CHAR(1),
    CORR_SEND_DATE          CHAR(14),
    IDENTIF                 CHAR(14),
    SPECIAL_CHEQUE_CHAR     VARCHAR(4),
    TRANSACTION_ID          VARCHAR(8),
    FILLER                  VARCHAR(10),
    PURCHASE_DATE           VARCHAR(11),
    IBAN                    VARCHAR(27)
);

