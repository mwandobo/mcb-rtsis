create table WS_CALL_LOG
(
    TRX_UNIT                INTEGER not null,
    TRX_DATE                DATE    not null,
    TRX_USR                 CHAR(8) not null,
    TRX_USR_SN              INTEGER not null,
    WS_CODE                 VARCHAR(20),
    ENTITY_CODE             VARCHAR(60),
    UNIT_DESCR              VARCHAR(300),
    ENTITY_TRANSACTION_ID   VARCHAR(60),
    ENTITY_TRANSACTION_DATE VARCHAR(40),
    ENTITY_PROTOCOL_NO      VARCHAR(60),
    ENTITY_PROTOCOL_DATE    DATE,
    ENTITY_REASON           VARCHAR(60),
    SERVER_HOST_NAME        VARCHAR(100),
    SERVER_HOST_IP          VARCHAR(20),
    END_USER_APPL_USER      VARCHAR(100),
    END_USER_HOST_NAME      VARCHAR(100),
    END_USER_HOST_IP        VARCHAR(20),
    END_USER_OS_USER        VARCHAR(100),
    constraint PK_WS_CALL_LOG
        primary key (TRX_USR_SN, TRX_USR, TRX_DATE, TRX_UNIT)
);

