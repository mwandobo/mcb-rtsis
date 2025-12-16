create table LOAN_MIGRATION
(
    ACC_TYPE     SMALLINT not null,
    ACC_SN       INTEGER  not null,
    ACC_CD       SMALLINT,
    LOAN_STATUS  CHAR(1),
    CAPITAL_AMNT DECIMAL(15, 2),
    EXPENSE_AMNT DECIMAL(15, 2),
    FK_UNITCODE  INTEGER  not null,
    PROCESSED    CHAR(1)  not null,
    constraint PKLNMIG
        primary key (FK_UNITCODE, ACC_SN, ACC_TYPE)
);

