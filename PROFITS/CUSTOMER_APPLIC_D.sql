create table CUSTOMER_APPLIC_D
(
    UNIT_CODE         INTEGER  not null,
    APPLICATION_ID    CHAR(15) not null,
    CUST_ID           INTEGER  not null,
    DETAIL_SN         INTEGER  not null,
    BENEF_NAME        CHAR(80),
    BENEF_ACCOUNT     CHAR(40),
    BENEF_AMOUNT      DECIMAL(15, 2),
    BENEF_BANK_SN     INTEGER,
    BENEF_BANK_DESC   CHAR(40),
    BENEF_BRANCH_SN   INTEGER,
    BENEF_BRANCH_DESC CHAR(40),
    BENEF_COMMENTS    CHAR(180),
    DATE1             DATE,
    DATE2             DATE,
    DATE3             DATE,
    AMOUNT1           DECIMAL(15, 2),
    AMOUNT2           DECIMAL(15, 2),
    AMOUNT3           DECIMAL(15, 2),
    TEXT1             CHAR(40),
    TEXT2             CHAR(40),
    TEXT3             CHAR(40),
    constraint IXU_CUSAPP_001
        primary key (UNIT_CODE, APPLICATION_ID, CUST_ID, DETAIL_SN)
);

