create table IFRS9_GUARANTEES_AND_CLLTRLS
(
    INFORMATION_DT               DATE        not null,
    GUARANTEE_OR_COLLATERAL_ID   CHAR(40)    not null,
    INTERNAL_SN                  DECIMAL(10) not null,
    GUARANTEE_COLLAT_SUB_TYPE_CD CHAR(30),
    GUARANTEE_COLLATERAL_TYPE_CD CHAR(30),
    VALUATION_DT                 TIMESTAMP(6),
    VALUATION_AMT                DECIMAL(15, 5),
    PROPERTY_REGION              CHAR(3),
    REAL_ESTATE_TYPE             CHAR(3),
    COLLATERAL_AMT               DECIMAL(15, 2),
    primary key (INFORMATION_DT, GUARANTEE_OR_COLLATERAL_ID, INTERNAL_SN)
);

