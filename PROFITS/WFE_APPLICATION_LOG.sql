create table WFE_APPLICATION_LOG
(
    APPLICATION_ID        CHAR(40)     not null,
    LOG_COUNTER           DECIMAL(10)  not null,
    WFE_ACTION            CHAR(2),
    TRX_USER              CHAR(8)      not null,
    TRX_UNIT              INTEGER      not null,
    TRX_DATE              DATE         not null,
    TRX_TMSTAMP           TIMESTAMP(6) not null,
    TRX_COMMENTS          VARCHAR(2048),
    REQUEST_HDR_ID        DECIMAL(10)  not null,
    REQUEST_CUST_ID       INTEGER,
    REQUEST_BRANCH        INTEGER,
    REQUEST_PRODUCT       INTEGER,
    REQUEST_AMN           DECIMAL(15, 2),
    REQUEST_CURRENCY      INTEGER,
    REQUEST_CHANNEL       INTEGER,
    PREVIOUS_ROLE         DECIMAL(10),
    PREVIOUS_STATUS       DECIMAL(10),
    PREVIOUS_ACTION       DECIMAL(10),
    PREVIOUS_USER         CHAR(8),
    CURRENT_ROLE          DECIMAL(10),
    CURRENT_STATUS        DECIMAL(10),
    CURRENT_USER          CHAR(8),
    PROCESSED_TRANSACT_ID DECIMAL(10),
    PROCESSED_WS_ID       VARCHAR(20),
    PROCESSED_WS_COMMAND  CHAR(80),
    constraint PK_WFE_APPL_LOG
        primary key (LOG_COUNTER, APPLICATION_ID)
);

