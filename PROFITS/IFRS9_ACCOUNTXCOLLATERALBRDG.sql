create table IFRS9_ACCOUNTXCOLLATERALBRDG
(
    REFERENCE_DTM              DATE     not null,
    GUARANTEE_OR_COLLATERAL_ID CHAR(40) not null,
    ACCOUNT_ID                 CHAR(32) not null,
    INTERNAL_SN                SMALLINT not null,
    CUSTOMER_ID                CHAR(32),
    ALLOCATED_MRK_VALUE        DECIMAL(15, 2),
    COLLATERAL_AMT             DECIMAL(15, 2),
    primary key (REFERENCE_DTM, GUARANTEE_OR_COLLATERAL_ID, ACCOUNT_ID, INTERNAL_SN)
);

