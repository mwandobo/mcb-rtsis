create table LG_BATCH_PARAMETER
(
    SN                     SMALLINT,
    PROGRAM_ID             CHAR(5),
    MONTH_TO               SMALLINT,
    MONTH_FROM             SMALLINT,
    LG_MONTH               SMALLINT,
    LG_DAYS                SMALLINT,
    LG_YEAR                SMALLINT,
    CURRENCY_FROM          INTEGER,
    PRODUCT_FROM           INTEGER,
    PRODUCT_TO             INTEGER,
    FGN_EXCHANGE_TYPE      INTEGER,
    UNIT_FROM              INTEGER,
    CURRENCY_TO            INTEGER,
    UNIT_TO                INTEGER,
    CUSTOMER_TO            INTEGER,
    CUSTOMER_FROM          INTEGER,
    NUMBER_FROM            DECIMAL(13),
    NUMBER_TO              DECIMAL(13),
    DATE_FROM              DATE,
    DATE_TO                DATE,
    ON_REQUEST_FLAG        CHAR(1),
    CUSTOMER_TYPE          CHAR(1),
    ACCOUNT_STATUS         CHAR(1),
    OBLIGATION_STATUS_FROM CHAR(1),
    OBLIGATION_STATUS_TO   CHAR(1),
    JOURNAL_TO             CHAR(2),
    JOURNAL_FROM           CHAR(2),
    GLG_PROCESS_ACCNT      VARCHAR(50)
);

create unique index IXP_LG__000
    on LG_BATCH_PARAMETER (SN, PROGRAM_ID);

