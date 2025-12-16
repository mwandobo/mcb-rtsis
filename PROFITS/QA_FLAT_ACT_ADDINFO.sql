create table QA_FLAT_ACT_ADDINFO
(
    WLT_DATE         DATE     not null,
    TRX_DATE         DATE     not null,
    TRX_UNIT         INTEGER  not null,
    TRX_USR          CHAR(8)  not null,
    TRX_SN           INTEGER  not null,
    TRX_INTERNAL_SN  SMALLINT not null,
    BENE             VARCHAR(40),
    BENE_ACCT        VARCHAR(35),
    BENE_ADDRESS     VARCHAR(120),
    BENE_CITY        VARCHAR(40),
    BENE_BANK_ID     CHAR(12),
    INTERMEDIARY_ID  CHAR(12),
    BY_ORDER         VARCHAR(40),
    BY_ORDER_ACCT    VARCHAR(35),
    BY_ORDER_ADDRESS VARCHAR(120),
    BY_ORDER_CITY    VARCHAR(40),
    BY_ORDER_COUNTRY CHAR(11),
    BY_ORDER_BANK_ID CHAR(12),
    BENE_CUST_ID     VARCHAR(35),
    BY_ORDER_CUST_ID VARCHAR(35),
    primary key (WLT_DATE, TRX_DATE, TRX_UNIT, TRX_USR, TRX_SN, TRX_INTERNAL_SN)
);

