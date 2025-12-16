create table TEST_LOAN_ACCOUNT
(
    ACC_UNIT           INTEGER  not null,
    ACC_TYPE           SMALLINT not null,
    ACC_SN             INTEGER  not null,
    ACC_CD             SMALLINT,
    LOAN_STATUS        CHAR(1),
    ACC_MECHANISM      CHAR(1),
    NORMAL_AMOUNT      DECIMAL(15, 2),
    OVER1_AMOUNT       DECIMAL(15, 2),
    OVER2_AMOUNT       DECIMAL(15, 2),
    DELAY_AMOUNT       DECIMAL(15, 2),
    NORMAL_RATE        DECIMAL(8, 4),
    OVER_RATE          DECIMAL(8, 4),
    PENALTY            DECIMAL(8, 4),
    NRM_REAL_RATE      DECIMAL(8, 4),
    OVER1_REAL_RATE    DECIMAL(8, 4),
    OVER2_REAL_RATE    DECIMAL(8, 4),
    DELAY_REAL_RATE    DECIMAL(8, 4),
    TOTAL_REAL_RATE    DECIMAL(8, 4),
    DURATION_PARAMETER CHAR(1),
    DURATION_ACCOUNT   CHAR(1),
    PROD_CATEG_ACC     INTEGER,
    PROD_CATEG_PARAMET INTEGER,
    NUMERATOR          DECIMAL(15, 2),
    DENOMINATOR        DECIMAL(15, 2),
    constraint TEST1
        primary key (ACC_TYPE, ACC_SN, ACC_UNIT)
);

