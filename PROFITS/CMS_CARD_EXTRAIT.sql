create table CMS_CARD_EXTRAIT
(
    CMS_EXTRAIT_SN   DECIMAL(10) not null,
    CARD_SN          DECIMAL(10) not null,
    ATM_DATE         DATE,
    TUN_DATE         DATE,
    TUN_UNIT         INTEGER,
    TUN_USER         CHAR(8),
    TUN_USR_SN       INTEGER,
    DB_IND           CHAR(1),
    CR_IND           CHAR(1),
    TRANSACTION_AMNT DECIMAL(15, 2),
    ACCOUNT_NUMBER   CHAR(40),
    ACCOUNT_CD       SMALLINT    not null,
    PRFT_SYSTEM      SMALLINT,
    ACCOUNT_NUM_TO   CHAR(40),
    ACCOUNT_CD_TO    SMALLINT,
    PRFT_SYS_TO      SMALLINT,
    ISO_REF_NUM      CHAR(20),
    MTI_CODE         CHAR(6),
    PROCESS_CD       CHAR(6),
    TMSTAMP          TIMESTAMP(6),
    ATM_TIME         TIME,
    AUDIT_NUMBER     INTEGER,
    constraint PK_CARD_EXTRAIT
        primary key (CARD_SN, CMS_EXTRAIT_SN)
);

