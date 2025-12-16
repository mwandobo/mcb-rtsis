create table VAULT_LOG
(
    TMSTAMP               TIMESTAMP(6) not null,
    TRX_UNIT              INTEGER      not null,
    TRX_USER              CHAR(8)      not null,
    TRX_DATE              DATE         not null,
    TRX_SN                INTEGER      not null,
    TRX_INTERNAL_SN       SMALLINT     not null,
    VAULT_TRX_TYPE        CHAR(2),
    VAULT_ACCESS_TYPE     CHAR(2),
    VAULT_UNIT            INTEGER,
    VAULT_ITEM_SN         DECIMAL(12),
    ITEM_DAYS_IN_VAULT    DECIMAL(10),
    ITEM_DAYS_NOTIN_VAULT DECIMAL(10),
    ITEM_USER             CHAR(8),
    PROCESS_REASON        CHAR(40),
    PROCESS_DETAIL        VARCHAR(500),
    CASH_AMOUNT           DECIMAL(15, 2),
    CASH_CURRENCY         INTEGER,
    constraint PK_VAULT_LOG
        primary key (TRX_INTERNAL_SN, TRX_SN, TRX_DATE, TRX_USER, TRX_UNIT, TMSTAMP)
);

