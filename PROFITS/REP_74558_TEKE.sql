create table REP_74558_TEKE
(
    PRFT_ACC_NUM       CHAR(40)       not null,
    PRFT_ACC_CD        SMALLINT       not null,
    PRFT_SYSTEM        SMALLINT       not null,
    CUST_ID            INTEGER        not null,
    C_DIGIT            SMALLINT,
    PRODUCT_ID         INTEGER        not null,
    TEIRESIAS_PROD_ID  CHAR(1)        not null,
    CUST_TYPE          CHAR(1),
    OV_ACC_BOOK_BAL    DECIMAL(15, 2) not null,
    BENEFS_NO          SMALLINT       not null,
    GUARANTORS_NO      SMALLINT       not null,
    CUST_IS_BENEF      CHAR(1)        not null,
    MAIN_BENEF         CHAR(1),
    MAIN_BENEF_CREDIT  DECIMAL(15, 2),
    TOTAL_OV_BOOK_BAL  DECIMAL(15, 2),
    SSX_PRFT_PRD_CONN  CHAR(1),
    OV_ACCRUAL_AMN     DECIMAL(15, 2),
    TOT_OV_ACCRUAL_AMN DECIMAL(15, 2),
    constraint IXU_REP_026
        primary key (CUST_ID, PRFT_SYSTEM, PRFT_ACC_NUM)
);

