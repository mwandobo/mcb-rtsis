create table DHSSE_INCOMING
(
    IDENTIFIER          INTEGER,
    CORR_SEND_DATE      DATE,
    CORR_SETTLE_DATE    DATE,
    PAY_ACCNO           CHAR(23),
    CHQ_NO              CHAR(20),
    BUY_BANK_CODE       CHAR(3),
    IBAN_CD             SMALLINT,
    ONL_TUN_INTERNAL_SN SMALLINT,
    ID_JUSTIFIC         INTEGER,
    ONL_TUN_UNIT        INTEGER,
    ONL_TUN_USR_SN      INTEGER,
    ONL_TUN_DATE        DATE,
    SETTLE_REF          CHAR(1),
    ENTRY_STATUS        CHAR(1),
    IBAN_CNTRY          CHAR(2),
    ACC_CODE            CHAR(3),
    IBAN_DIGITS         CHAR(4),
    PAY_BRANCH          CHAR(8),
    BUY_BRANCH          CHAR(8),
    PAY_CURRENCY        CHAR(5),
    DHSSE_REF           CHAR(7),
    ISS_DATE            CHAR(8),
    TRAN_CODE           CHAR(8),
    TRAN_DATE           CHAR(8),
    AMOUNT              CHAR(15),
    FILENAME            CHAR(20),
    ONL_TUN_USR         VARCHAR(8),
    ERROR_DESCR         CHAR(80),
    ONL_TMSTAMP         TIMESTAMP(6),
    FILE_SN             SMALLINT,
    RESPONSE_DATE       DATE,
    INCLUDE_IN_FILE     CHAR(1),
    PROCESSING_ID       DECIMAL(10),
    REPRESENTATION_FLG  CHAR(1),
    OVERDRAWN_FLG       CHAR(1),
    FILE_DATA           VARCHAR(300),
    PREV_ACCNO          CHAR(23),
    PREV_CHQ_NO         CHAR(20),
    PREV_AMOUNT         CHAR(15),
    PREV_PAY_CURRENCY   CHAR(5),
    UPDATED_FILE_DATA   VARCHAR(300),
    XML_FILE_NAME       CHAR(100),
    XML_FILE_ID         DECIMAL(15)
);

create unique index IXN_DHS_001
    on DHSSE_INCOMING (CORR_SEND_DATE);

create unique index IXN_DHS_002
    on DHSSE_INCOMING (PAY_ACCNO, CHQ_NO, BUY_BANK_CODE);

create unique index IXU_DHS_001
    on DHSSE_INCOMING (IDENTIFIER, CORR_SEND_DATE, CORR_SETTLE_DATE, PAY_ACCNO, CHQ_NO, BUY_BANK_CODE);

