create table ADDITIONAL_TRX
(
    TMSTAMP_PROC      TIMESTAMP(6),
    FORECAST_TMSTAMP  TIMESTAMP(6),
    FORECAST_STATUS   VARCHAR(1),
    FORECAST_RESULT   VARCHAR(80),
    PROCESS_USER      VARCHAR(8),
    FORECAST_USER     VARCHAR(8),
    UPLOAD_TMSTAMP    TIMESTAMP(6),
    UPLOAD_USER       VARCHAR(8),
    AUTH_USER         VARCHAR(8),
    AUTH_TMSTAMP      TIMESTAMP(6),
    AUTH_STATUS       VARCHAR(1),
    AUTH_REASON       CHAR(80),
    INTERNAL_SN       INTEGER  not null,
    UNIT              INTEGER  not null,
    DATE_REC          DATE     not null,
    TRX_USR           CHAR(8),
    TMSTAMP           TIMESTAMP(6),
    TRX_UNIT          INTEGER,
    TRX_DATE          DATE,
    AMOUNT            DECIMAL(15, 2),
    GLG_ACCOUNT_ID    CHAR(21),
    ID_JUSTIFIC       INTEGER,
    DEP_ACCOUNT       DECIMAL(11),
    DR_CR_FLAG        SMALLINT,
    ENTRY_STATUS      SMALLINT,
    RATE              DECIMAL(12, 6),
    DC_AMOUNT         DECIMAL(15, 2),
    VALEUR_DATE       DATE,
    COMMENTS          CHAR(40),
    ISO_CURRENCY      CHAR(5),
    ID_CURRENCY       INTEGER,
    FILE_NAME         CHAR(20) not null,
    TC_CODE           DECIMAL(10),
    AVAIL_DATE        DATE,
    CUST_ID           INTEGER,
    ERROR_DESCRIPTION CHAR(80),
    TUN_USER          CHAR(8),
    TUN_USR_SN        INTEGER,
    ARTICLE_SN        DECIMAL(15) default 0,
    PROGRAM_ID_GROUP  CHAR(5),
    constraint PK_PAYROLL
        primary key (FILE_NAME, DATE_REC, UNIT, INTERNAL_SN)
);

create unique index "add003V_index"
    on ADDITIONAL_TRX (TRX_DATE, INTERNAL_SN, ENTRY_STATUS, AUTH_STATUS);

