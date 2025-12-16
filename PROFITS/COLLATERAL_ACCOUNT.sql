create table COLLATERAL_ACCOUNT
(
    TMSTAMP            TIMESTAMP(6) not null,
    ACC_COLLATERAL_SN  SMALLINT     not null,
    ACC_UNIT           SMALLINT     not null,
    ACC_TYPE           SMALLINT     not null,
    ACC_SN             DECIMAL(15)  not null,
    FK_COLLATERALFK_CO INTEGER,
    FK_COLLATERALCOLLA DECIMAL(10),
    FK_CUSTOMERCUST_ID INTEGER,
    EST_VALUE_AMN      DECIMAL(15, 2),
    ENTRY_STS          CHAR(1)      not null,
    CHEQ_COLLAT_STS    CHAR(1),
    PROFITS_SYSTEM     INTEGER      not null,
    constraint PKCOLLAC
        primary key (ACC_SN, ACC_TYPE, ACC_UNIT, ACC_COLLATERAL_SN, TMSTAMP)
);

