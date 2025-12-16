create table HCP_PARAMETERS
(
    BANK_CODE          SMALLINT     not null,
    JUST_OL_CHRG       INTEGER,
    JUST_OL_CHRG_TAX   INTEGER,
    JUST_RET_CHRG      INTEGER,
    JUST_RET_CHRG_TAX  INTEGER,
    JUST_ITEM_CHRG     INTEGER,
    JUST_ITEM_CHRG_TAX INTEGER,
    JUST_OL_DEBIT      INTEGER,
    JUST_OL_CREDIT     INTEGER,
    REV_VOUCHER_ID     INTEGER,
    TMSTAMP            TIMESTAMP(6) not null,
    USR_UPD            CHAR(8),
    UPD_DTE            DATE,
    constraint PK_HCP_PARAMETERS
        primary key (TMSTAMP, BANK_CODE)
);

