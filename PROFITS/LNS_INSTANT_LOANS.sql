create table LNS_INSTANT_LOANS
(
    CUST_ID             INTEGER              not null,
    LOAN_ACCOUNT        CHAR(40)             not null,
    TMSTAMP             TIMESTAMP(6),
    LOANS_PROGRAM_ID    INTEGER,
    PROGRAM_CURRENCY    INTEGER,
    MOBILE_TEL          VARCHAR(15),
    LOAN_CD             INTEGER,
    AGREEMENT_ACCOUNT   CHAR(40)             not null,
    AGREEMENT_CD        INTEGER,
    DEPOSIT_ACCOUNT     CHAR(40)             not null,
    DEPOSIT_CD          INTEGER,
    C_DIGIT             INTEGER,
    INSTANT_LOAN_AMOUNT DECIMAL(15, 2),
    INSTALL_FREQ        INTEGER,
    INSTALL_COUNT       INTEGER,
    ACC_OPEN_DT         DATE,
    ACC_EXP_DT          DATE,
    TRX_USER            CHAR(8),
    BANKEMPLOYEE        CHAR(8),
    ALLOCATION_TYPE     CHAR(1),
    COLLATERAL_SN       BIGINT,
    DRAWDOWN_REQUEST_ID CHAR(15),
    LOAN_CAPITAL        DECIMAL(15, 2),
    LOAN_INTEREST       DECIMAL(15, 2),
    LOAN_EXPENCES       DECIMAL(15, 2),
    LOAN_COMMISSION     DECIMAL(15, 2),
    DEPOSIT_AMOUNT      DECIMAL(15, 2),
    TRX_COMMENTS        CHAR(40),
    APPLICATION_ID      CHAR(20) default '0' not null,
    APPLICATION_TYPE    INTEGER  default 0   not null,
    constraint INSTANT_LOANS
        primary key (APPLICATION_TYPE, APPLICATION_ID, LOAN_ACCOUNT, CUST_ID, DEPOSIT_ACCOUNT)
);

create unique index INSTANT_LOANS
    on LNS_INSTANT_LOANS (LOAN_ACCOUNT, CUST_ID, AGREEMENT_ACCOUNT);

