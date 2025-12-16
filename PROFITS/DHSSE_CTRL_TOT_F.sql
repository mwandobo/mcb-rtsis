create table DHSSE_CTRL_TOT_F
(
    PROGRAM_ID       CHAR(5)  not null,
    ACH_SETTLE_DATE  CHAR(8)  not null,
    CUTOFF_SN        SMALLINT not null,
    CURRENCY_ID      INTEGER  not null,
    ACH_SETTLE_DATE2 CHAR(8),
    ACH_CTRL_NUM     CHAR(6),
    ACH_CTRL_AMOUNT  CHAR(15),
    ACH_BANK_CODE    CHAR(3)  not null,
    ENTRY_STATUS     CHAR(1),
    TMSTAMP          TIMESTAMP(6),
    FILENAME         CHAR(40),
    SETTL_PROCESSED  CHAR(1),
    GROUP_ID         INTEGER default 0,
    constraint IXU_DHS_004
        primary key (CURRENCY_ID, CUTOFF_SN, PROGRAM_ID, ACH_SETTLE_DATE, ACH_BANK_CODE)
);

