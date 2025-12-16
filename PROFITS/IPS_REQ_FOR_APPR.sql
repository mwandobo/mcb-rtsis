create table IPS_REQ_FOR_APPR
(
    FILE_ID                 CHAR(16)    not null,
    FILE_RECORD_SN          DECIMAL(15) not null,
    FILE_DATETIME           CHAR(8),
    RECORD_TYPE             SMALLINT,
    INSTRUCTION_REFERENCE   CHAR(16),
    CUSTOMER_NAME           CHAR(35),
    CUSTOMER_ACCOUNT_IBAN   CHAR(34),
    CUSTOMER_ACCOUNT_PRFT   CHAR(40),
    AMOUNT1_SIGN            CHAR(1),
    AMOUNT1                 CHAR(17),
    AMOUNT1_NUM             DECIMAL(15, 2),
    AMOUNT2_SIGN            CHAR(1),
    AMOUNT2                 CHAR(17),
    AMOUNT2_NUM             DECIMAL(15, 2),
    SETTLEMENT_DATE         CHAR(8),
    REMMITTANCE_INFORMATION CHAR(70),
    SWITCHING_CSM           CHAR(11),
    RFA_GROUP               CHAR(2),
    REPLY_CUTOFF_DATE       CHAR(8),
    REPLY_CUTOFF_TIME       CHAR(4),
    REC_COUNT               INTEGER,
    ERROR_DESCR             CHAR(80),
    REPLY_FLAG              CHAR(1),
    RESULT_FLAG             CHAR(1),
    CRE_DEB_DATE            DATE,
    SENT_DATE               DATE,
    SENT_DATE_FILE_NO       SMALLINT,
    constraint PKIPSREQFORAP
        primary key (FILE_RECORD_SN, FILE_ID)
);

