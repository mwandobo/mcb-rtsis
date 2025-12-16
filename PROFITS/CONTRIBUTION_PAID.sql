create table CONTRIBUTION_PAID
(
    ACCOUNT_NUMBER CHAR(40),
    ACCOUNT_CD     SMALLINT,
    CUST_ID        INTEGER,
    C_DIGIT        SMALLINT,
    ACC_SN         INTEGER  not null,
    ACC_TYPE       SMALLINT not null,
    ACC_UNIT       INTEGER  not null,
    REQUEST_SN     SMALLINT,
    REQUEST_TYPE   CHAR(1),
    TOTAL_INT      DECIMAL(15, 2),
    TOTAL_CONTR    DECIMAL(15, 2),
    CONTR_PERC     DECIMAL(15, 2),
    INT_PAID       DECIMAL(15, 2),
    CONTR_PAID     DECIMAL(15, 2)
);

