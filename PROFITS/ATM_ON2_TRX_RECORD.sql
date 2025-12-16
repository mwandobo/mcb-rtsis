create table ATM_ON2_TRX_RECORD
(
    REVERSED_FLAG      CHAR(1)     not null,
    SOURCE_ACCOUNT     DECIMAL(10) not null,
    SOURCE_BUSINESS_DA DATE        not null,
    SOURCE_FI_NBR      SMALLINT    not null,
    SOURCE_SEQ_NUMBER  DECIMAL(10) not null,
    SOURCE_TERM_NBR    DECIMAL(10) not null,
    TUN_DATE           DATE        not null,
    TUN_UNIT           INTEGER     not null,
    TUN_USER_SN        DECIMAL(10) not null,
    TUN_USR            VARCHAR(8)  not null,
    ACC_UNIT           INTEGER,
    ATM_UNIT           INTEGER,
    DEST_ACCOUNT       DECIMAL(10),
    REJECTED_ITEMS_ACC DECIMAL(10),
    LOAN_ACCOUNT_NBR   DECIMAL(13),
    JOURNAL_AMOUNT     DECIMAL(15, 2),
    CASH_AMOUNT        DECIMAL(15, 2),
    TMSTAMP            DATE,
    constraint IXU_ATM_039
        primary key (REVERSED_FLAG, SOURCE_ACCOUNT, SOURCE_BUSINESS_DA, SOURCE_FI_NBR, SOURCE_SEQ_NUMBER,
                     SOURCE_TERM_NBR, TUN_DATE, TUN_UNIT, TUN_USER_SN, TUN_USR)
);

