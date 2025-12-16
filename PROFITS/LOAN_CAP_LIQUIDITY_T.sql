create table LOAN_CAP_LIQUIDITY_T
(
    ACC_CURRENCY        INTEGER not null,
    ACC_LIQ_MECH        CHAR(1) not null,
    ACC_UNIT            INTEGER not null,
    ID_PRODUCT          INTEGER not null,
    EXP_NOW_AMN         DECIMAL(15, 2),
    TOT_FC_AMN          DECIMAL(15, 2),
    OVERDUE_AMN         DECIMAL(15, 2),
    TOT_LC_AMN          DECIMAL(15, 2),
    EXP_2_7_DAYS_AMN    DECIMAL(15, 2),
    EXP_8_30_DAYS_AMN   DECIMAL(15, 2),
    EXP_1_3_MONTHS_AMN  DECIMAL(15, 2),
    EXP_3_6_MONTHS_AMN  DECIMAL(15, 2),
    EXP_6_12_MONTHS_AMN DECIMAL(15, 2),
    EXP_OVER_YEAR_AMN   DECIMAL(15, 2),
    ACC_CURR_DESC       CHAR(5),
    PRODUCT_DESC        CHAR(40),
    ACC_LIQ_MECH_DESC   VARCHAR(20),
    UNIT_DESC           VARCHAR(30),
    constraint IXU_LNS_023
        primary key (ACC_CURRENCY, ACC_LIQ_MECH, ACC_UNIT, ID_PRODUCT)
);

