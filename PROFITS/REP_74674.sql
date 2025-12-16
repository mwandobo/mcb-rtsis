create table REP_74674
(
    UNIT_CODE       INTEGER       not null,
    PRODUCT_ID      INTEGER       not null,
    INT_ID          INTEGER       not null,
    INT_PERC        DECIMAL(8, 4) not null,
    PROD_ACC_TYPE   INTEGER,
    MID_INT         DECIMAL(9, 6),
    ACC_TOT_CNT     INTEGER,
    NRM_ACC_BAL     DECIMAL(15, 2),
    OV_ACC_BAL      DECIMAL(15, 2),
    TOT_ACC         DECIMAL(15, 2),
    MID_TOT_ACC     DECIMAL(15, 2),
    FIXED_FLOAT_FLG CHAR(1),
    UNIT_NAME       VARCHAR(40),
    PROD_DESC       VARCHAR(40),
    INT_DESC        VARCHAR(40),
    PROD_TYPE_DESC  VARCHAR(40),
    constraint IXU_REP_034
        primary key (UNIT_CODE, PRODUCT_ID, INT_ID, INT_PERC)
);

