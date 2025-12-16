create table TBL_FOR_REP_ST04
(
    CUST_ID              INTEGER not null
        constraint IXU_REP_168
            primary key,
    CUST_CD              SMALLINT,
    CUST_GROUP           INTEGER,
    CUST_ACTIVITY        INTEGER,
    GROUP_CUST_ID        INTEGER,
    SUM_EUR_BALANCE      DECIMAL(15, 2),
    PRFT_ACC_LMT_AMN     DECIMAL(15, 2),
    SUM_COLLATERAL       DECIMAL(15, 2),
    SUM_EUR_OVERDUE      DECIMAL(15, 2),
    SUM_EUR_LG           DECIMAL(15, 2),
    LNS_POSITIVE_AMN_EUR DECIMAL(15, 2),
    PRINT_FLG            CHAR(1),
    PREV_RATE            CHAR(10),
    CURR_RATE            CHAR(10),
    CUST_FIRSTNAME       CHAR(20),
    CUST_AFM             CHAR(20),
    GROUP_AFM            CHAR(20),
    CUST_GROUP_NAME      CHAR(40),
    GROUP_SURNAME        CHAR(40),
    GROUP_CUST_SURNAME   CHAR(70),
    CUST_SURNAME         CHAR(70)
);

