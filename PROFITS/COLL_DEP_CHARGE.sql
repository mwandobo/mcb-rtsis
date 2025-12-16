create table COLL_DEP_CHARGE
(
    COLLATERAL_UNIT INTEGER     not null,
    COLLATERAL_TP   INTEGER     not null,
    COLLATERAL_SN   DECIMAL(10) not null,
    DEP_ACC_NUMBER  DECIMAL(11) not null,
    VAULT_SN        DECIMAL(11) not null,
    TRX_DATE        DATE        not null
);

create unique index IXU_COL_043
    on COLL_DEP_CHARGE (COLLATERAL_UNIT, COLLATERAL_TP, COLLATERAL_SN, DEP_ACC_NUMBER, VAULT_SN, TRX_DATE);

