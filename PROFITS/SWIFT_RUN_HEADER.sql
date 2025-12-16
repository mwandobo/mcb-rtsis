create table SWIFT_RUN_HEADER
(
    PRFT_REF_NO       CHAR(16) not null
        constraint IXU_SWI_009
            primary key,
    TUN_INTERNAL_SN   SMALLINT,
    TRX_UNIT          INTEGER,
    ID_TRANSACT       INTEGER,
    TRANS_UNIT_CODE   INTEGER,
    ISSUE_UNIT_CODE   INTEGER,
    OUTGOING_ORDER    INTEGER,
    TRX_SN            INTEGER,
    TRX_DATE          DATE,
    CONF_PROC_TMSTAMP TIMESTAMP(6),
    SEND_TMSTAMP      TIMESTAMP(6),
    TMSTAMP           TIMESTAMP(6),
    STATUS            CHAR(1),
    STP_INDICATION    CHAR(1),
    PRIORITY_CODE     CHAR(1),
    MANUAL_SWIFT      CHAR(1),
    MSG_CATEGORY      CHAR(1),
    ISSUE_USER        CHAR(8),
    CONF_PROC_USER    CHAR(8),
    SEND_USER         CHAR(8),
    TRX_USR           CHAR(8),
    SIGNB_USER        CHAR(8),
    TRX_REF_NO_20     CHAR(16),
    MESSAGE_TYPE      CHAR(20),
    COMMENTS          CHAR(200),
    SENDER_BIC_ADDR   VARCHAR(12),
    RECEIVER_BIC_ADDR VARCHAR(12)
);

