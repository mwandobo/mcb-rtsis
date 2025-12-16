create table LNS_MULTI_TRX_SETUP
(
    ENTRY_STATUS            CHAR(1),
    LNS_CHARACTERISTICS_FLG CHAR(1),
    TOP_UP_FLG              CHAR(1),
    BRIDGING_FLG            CHAR(1),
    INSTANT_PROGRAM_ID      INTEGER,
    AUTO_POPULATE           CHAR(1),
    CUSTOMER_NEEDED         CHAR(1),
    JUST_DESCR              CHAR(50),
    TRX_DESCR               CHAR(50),
    AMOUNT_NEEDED           CHAR(1),
    PROGRAM_SN              INTEGER  not null,
    DESCRIPTION             VARCHAR(50),
    TRAN_SERIAL             SMALLINT not null,
    PRFT_SYSTEM             INTEGER,
    TRX_TRANSACT            INTEGER,
    TRX_JUSTIFIC            INTEGER,
    ACCOUNT_NEEDED          CHAR(1),
    AMOUNT_CALC             CHAR(1),
    GUAR_AMOUNT_CAL         CHAR(1),
    BOOST_AMOUNT_CAL        CHAR(1),
    KEEP_AMOUNT             CHAR(1),
    USE_KEEP_AMOUNT         CHAR(1),
    MULTI_TRANSACTION       CHAR(1),
    DEP_BOOST_FLG           CHAR(1),
    PRODUCT_NEEDED          CHAR(1),
    CURRENCY_NEEDED         CHAR(1),
    INSTANT_MECHANISM       INTEGER,
    STEP_MANDATORY          CHAR(1),
    BATCH_FLG               CHAR(1),
    DCD_RULE_ID             DECIMAL(12),
    DCD_SYSTEM              SMALLINT,
    LNS_CHARACTERISTICS_DIS CHAR(30),
    LEASING_FLG             CHAR(1),
    FUNCTION_SELECTION_FLG  CHAR(50),
    OUTPUT_LINE             CHAR(1),
    REVERSAL_ALLOWED        CHAR(1),
    LINE_PERCENTAGE         DECIMAL(15, 2)
);

create unique index MULTI_SETUP_IDX1
    on LNS_MULTI_TRX_SETUP (PROGRAM_SN, TRAN_SERIAL);

alter table LNS_MULTI_TRX_SETUP
    add constraint MULTI_SETUP_PRK
        primary key (PROGRAM_SN, TRAN_SERIAL);

