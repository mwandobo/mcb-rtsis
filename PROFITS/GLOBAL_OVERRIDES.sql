create table GLOBAL_OVERRIDES
(
    ID                DECIMAL(15) not null,
    GLOBAL_ORDER_CODE VARCHAR(50) not null,
    SN                INTEGER     not null,
    PRFT_SYSTEM       SMALLINT,
    ACCOUNT_NUMBER    CHAR(40),
    ORDER_AMOUNT      DECIMAL(18, 2),
    ORDER_CURRENCY    INTEGER,
    OVERRIDE_TYPE     CHAR(2),
    OVERRIDE_REASON   CHAR(250),
    ID_TRANSACT       INTEGER,
    ID_JUSTIFIC       INTEGER,
    CREATE_UNIT       INTEGER,
    CREATE_DATE       DATE,
    CREATE_USR        CHAR(8),
    CREATE_TMSTAMP    TIMESTAMP(6),
    USED_UNIT         INTEGER,
    USED_DATE         DATE,
    USED_USR          CHAR(8),
    USED_TMSTAMP      TIMESTAMP(6),
    ENTRY_STATUS      CHAR(1),
    DEAL_USR_CODE     VARCHAR(8),
    DEAL_REF_NO       VARCHAR(20),
    CURRENCY_RATE     DECIMAL(12, 6),
    CONVERT_AMOUNT    DECIMAL(18, 2),
    constraint PK_GLOBAL_OVER
        primary key (SN, GLOBAL_ORDER_CODE, ID)
);

