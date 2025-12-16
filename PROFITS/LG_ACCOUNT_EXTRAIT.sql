create table LG_ACCOUNT_EXTRAIT
(
    ACC_UNIT           INTEGER      not null,
    ACC_TYPE           SMALLINT     not null,
    ACC_SN             DECIMAL(13)  not null,
    TMSTAMP            TIMESTAMP(6) not null,
    TRX_INTERNAL_SN    SMALLINT     not null,
    ACC_CD             SMALLINT,
    TRX_UNIT           INTEGER,
    TRX_USR            CHAR(8),
    TRX_DATE           DATE,
    TRX_SN             INTEGER,
    ENTRY_STATUS       CHAR(1),
    TRANSACTION_CODE   INTEGER,
    JUSTIFICATION_CODE INTEGER,
    VALEUR_DT          DATE,
    TRX_AMN            DECIMAL(15, 2),
    TRX_CURR           INTEGER      not null,
    CHR_CURR           INTEGER,
    ACC_CURR           INTEGER      not null,
    LG_AMN             DECIMAL(15, 2),
    EXPENSE_AMN        DECIMAL(15, 2),
    COMMISSION_AMN     DECIMAL(15, 2),
    EXP_IN_CC_AMN      DECIMAL(15, 2),
    COM_IN_CC_AMN      DECIMAL(15, 2),
    PRV_ACCOUNT_BAL    DECIMAL(15, 2),
    EXTRAIT_COMMENTS   VARCHAR(40),
    constraint PKACCOUNTEXTRAIT
        primary key (TRX_INTERNAL_SN, TMSTAMP, ACC_SN)
);

