create table CHEQUES_FOR_COLLECTION
(
    ENTRY_STATUS        CHAR(1)        not null,
    TRX_DATE            DATE,
    IDENTIFIER          DECIMAL(13)    not null,
    CHEQUE_NUMBER       VARCHAR(20)    not null,
    ISSUE_DATE          DATE           not null,
    CHEQUE_AMOUNT       DECIMAL(15, 2) not null,
    CHEQUE_AMOUNT_LC    DECIMAL(15, 2),
    SUBSYSTEM           CHAR(2),
    CHEQUE_ACC_NUMBER   CHAR(23),
    CHEQUE_DRAWN_BY     VARCHAR(40),
    DRAWN_BANK          VARCHAR(40),
    CHEQUE_TYPE         CHAR(1)        not null,
    COLLECTION_FLAG     CHAR(1)        not null,
    BEARER_NAME         VARCHAR(40),
    BEARER_ADDRESS1     VARCHAR(40),
    BEARER_ADDRESS2     VARCHAR(40),
    BEARER_CITY         VARCHAR(30),
    BEARER_ZIP_CODE     CHAR(10),
    BEARER_TAX_REG_NO   VARCHAR(20),
    BEARER_ID_TYPE      CHAR(1),
    BEARER_ID_NUM       VARCHAR(20),
    CFC_BILL_SERIAL_NUM DECIMAL(10),
    BILL_SERIAL_NUM     DECIMAL(10),
    ENTRY_COMMENTS      VARCHAR(40),
    FK_COLLABORATIOBAN  INTEGER        not null,
    constraint PCOLLECT
        primary key (FK_COLLABORATIOBAN, ISSUE_DATE, CHEQUE_NUMBER, IDENTIFIER)
);

