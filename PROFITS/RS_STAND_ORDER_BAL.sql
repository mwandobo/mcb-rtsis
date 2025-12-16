create table RS_STAND_ORDER_BAL
(
    P_ACCOUNT        CHAR(30) not null,
    LOAN_ACC_SN      INTEGER  not null,
    LOAN_ACC_TYPE    SMALLINT not null,
    LOAN_ACC_UNIT    INTEGER  not null,
    P_BANK_DATE      DATE     not null,
    P_ERRCODE        SMALLINT,
    P_MOD_ID         SMALLINT,
    P_PROD_AUTOKEY   VARCHAR(13),
    P_CL_BRANCHCODE  CHAR(5),
    P_CL_CODE        CHAR(20),
    P_TAXCODE        CHAR(20),
    P_ACC_BRANCHCODE CHAR(5),
    P_CURRENCY       CHAR(3),
    P_AGREEMENT_NUM  CHAR(30),
    P_STAT_EXP       SMALLINT,
    P_AMOUNT         DECIMAL(15, 2),
    P_PAYMENT_DATE   DATE,
    P_OVERDDAYS      INTEGER,
    P_COLLECT        CHAR(15),
    P_COLL_PRIORITY  SMALLINT,
    P_RISK_GROUP     SMALLINT,
    P_PROD_NUM       INTEGER,
    P_CHANNEL        CHAR(20),
    P_DESCRIPTION    VARCHAR(1024),
    P_PAY_TYPE       SMALLINT,
    PRFT_SYSTEM      SMALLINT,
    COLLECTED_AMN    DECIMAL(15, 2),
    PROCESSED_FLAG   CHAR(1),
    TRX_DATE         DATE,
    TMSTAMP          TIMESTAMP(6),
    TRX_UNIT         INTEGER,
    TRX_USR          CHAR(8),
    TRX_SN           INTEGER,
    CUST_ID          INTEGER,
    DIFF_AMOUNT      DECIMAL(15, 2),
    MORNING_AMN      DECIMAL(15, 2),
    constraint PKRS_ST_ORD_BAL
        primary key (LOAN_ACC_UNIT, LOAN_ACC_TYPE, LOAN_ACC_SN, P_ACCOUNT, P_BANK_DATE)
);

create unique index IXN_RS_001
    on RS_STAND_ORDER_BAL (CUST_ID);

