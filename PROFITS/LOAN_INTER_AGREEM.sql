create table LOAN_INTER_AGREEM
(
    ACC_SN              INTEGER  not null,
    ACC_TYPE            SMALLINT not null,
    ACC_UNIT            INTEGER  not null,
    TRX_DATE            DATE     not null,
    PRODUCT_ID          INTEGER,
    OV_SPRD_INTEREST    DECIMAL(15, 2),
    OV_N128_INTEREST    DECIMAL(15, 2),
    OV_DB_INTEREST      DECIMAL(15, 2),
    TOT_SPRD_INTEREST   DECIMAL(15, 2),
    TOT_N128_INTEREST   DECIMAL(15, 2),
    TOT_DB_INTEREST     DECIMAL(15, 2),
    OVERDUE_BALANCE     DECIMAL(15, 2),
    OV_PENALTY_INTEREST DECIMAL(15, 2),
    ACCOUNT_BAL         DECIMAL(15, 2),
    VALEUR_DT           DATE,
    LOAN_STATUS         CHAR(1),
    ACC_MECHANISM       CHAR(1),
    constraint IXU_LOA_010
        primary key (ACC_SN, ACC_TYPE, ACC_UNIT, TRX_DATE)
);

