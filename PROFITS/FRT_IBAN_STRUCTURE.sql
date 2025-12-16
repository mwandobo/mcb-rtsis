create table FRT_IBAN_STRUCTURE
(
    MODIFICATION_FLAG  CHAR(1)      not null,
    RECORD_KEY         CHAR(12)     not null
        constraint PK_RECORD_KEY1
            primary key,
    IBAN_CN            CHAR(2)      not null,
    IBAN_CN_POS        CHAR(2)      not null,
    IBAN_CN_LEN        CHAR(1)      not null,
    IBAN_CHCK_DGT_POS  CHAR(2)      not null,
    IBAN_CHCK_DGT_LEN  CHAR(2)      not null,
    BANK_ID_POS        CHAR(2)      not null,
    BANK_ID_LEN        CHAR(2)      not null,
    BRANCH_ID_POS      CHAR(2),
    BRANCH_ID_LEN      CHAR(2)      not null,
    IBAN_NATID_LEN     CHAR(2)      not null,
    ACC_NUM_POS        CHAR(2)      not null,
    ACC_NUM_LEN        CHAR(2)      not null,
    IBAN_TOTAL_LEN     CHAR(2)      not null,
    IS_SEPA            CHAR(1)      not null,
    OPT_COMMENCE_DT    CHAR(8),
    OI_TRAN_TYPES      VARCHAR(70),
    MANDAT_COMMENCE_DT CHAR(8),
    MI_TRANS_TYPES     VARCHAR(70),
    IS_ISO13616        CHAR(1)      not null,
    ANF_RECORD_KEY     CHAR(12),
    REUSED_BY          VARCHAR(40),
    TMSTAMP            TIMESTAMP(6) not null
);

comment on table FRT_IBAN_STRUCTURE is 'DATA ABOUT SEPA COUNTRIES IBAN STRUCTURE';

create unique index SC_IBAN_CN
    on FRT_IBAN_STRUCTURE (IBAN_CN);

