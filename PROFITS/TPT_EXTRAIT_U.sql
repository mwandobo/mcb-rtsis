create table TPT_EXTRAIT_U
(
    TP_TYPE            CHAR(5)        not null,
    TP_NUMBER          DECIMAL(15)    not null,
    TP_TASK            CHAR(8)        not null,
    TMSTAMP            TIMESTAMP(6)   not null,
    TRX_INTERNAL_SN    SMALLINT       not null,
    TRX_CURRENCY       INTEGER        not null,
    TRX_DATE           DATE           not null,
    TRX_UNIT           INTEGER        not null,
    TRX_USR            CHAR(8)        not null,
    TRX_USR_SN         INTEGER        not null,
    INVOICE_NUMBER     CHAR(12),
    ISSUE_DATE         DATE,
    TRX_COMMENTS       VARCHAR(70),
    ID_JUSTIFIC        INTEGER,
    TRX_CODE           INTEGER,
    TRANSACTION_STATUS SMALLINT,
    PREV_BALANCE       DECIMAL(15, 2) not null,
    BAL_PARTICIPATION  SMALLINT,
    AMOUNT_TYPE        SMALLINT       not null,
    AMNT               DECIMAL(15, 2) not null,
    INVOICE_CNT        SMALLINT,
    CONSUMABLE_IND     SMALLINT,
    GOVERMENT_IND      SMALLINT,
    SALES_PURC_IND     SMALLINT,
    constraint IXU_CIU_058
        primary key (TRX_INTERNAL_SN, TMSTAMP, TP_TASK, TP_NUMBER, TP_TYPE)
);

