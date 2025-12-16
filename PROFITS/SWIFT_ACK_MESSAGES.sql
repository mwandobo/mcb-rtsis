create table SWIFT_ACK_MESSAGES
(
    N_ACK_MUR      CHAR(16) not null
        constraint SWT_N_ACK_MUR_PK
            primary key,
    TRX_DATE       DATE,
    TMSTAMP        TIMESTAMP(6),
    N_ACK_APPL_ID  CHAR(10),
    N_ACK_SERV_ID  CHAR(10),
    RECEIVER_BIC   CHAR(12),
    N_ACK_STATUS   SMALLINT,
    REJECTION_CODE CHAR(7),
    LOAD_STATUS    SMALLINT,
    FILE_NAME      CHAR(50),
    FULL_MESSAGE   VARCHAR(2048),
    FK_PRFT_REF_NO CHAR(16),
    COMMENTS       VARCHAR(80)
);

