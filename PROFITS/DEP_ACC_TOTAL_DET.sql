create table DEP_ACC_TOTAL_DET
(
    BENEF_CUST_ID      INTEGER     not null,
    TOT_INT_TAX_1      DECIMAL(15, 2),
    TOT_INT_TAX_2      DECIMAL(15, 2),
    TIMESTAMP          TIMESTAMP(6),
    FK_DEP_ACCOUNT_ACC DECIMAL(11) not null,
    FK_DEP_ACTUAL_YEAR SMALLINT    not null,
    constraint PK_DEP_ACC_TOT_DET
        primary key (FK_DEP_ACCOUNT_ACC, FK_DEP_ACTUAL_YEAR, BENEF_CUST_ID)
);

