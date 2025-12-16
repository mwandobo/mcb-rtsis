create table LOAN_CAP_LIQUIDITY
(
    ID_PRODUCT     INTEGER not null,
    EXPIRE_DT      DATE    not null,
    LOAN_STATUS    CHAR(1) not null,
    ACC_CURRENCY   INTEGER not null,
    ACC_LIQ_MECH   CHAR(1) not null,
    ACC_UNIT       INTEGER not null,
    CAPITAL_FC_AMN DECIMAL(15, 2),
    CAPITAL_LC_AMN DECIMAL(15, 2),
    constraint IXU_LNS_033
        primary key (ID_PRODUCT, EXPIRE_DT, LOAN_STATUS, ACC_CURRENCY, ACC_LIQ_MECH, ACC_UNIT)
);

