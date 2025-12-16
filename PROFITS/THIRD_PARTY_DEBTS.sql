create table THIRD_PARTY_DEBTS
(
    VALUER_DT    DATE        not null,
    ACCOUNT_NO   DECIMAL(11) not null,
    AGR_CODE     INTEGER     not null,
    TRX_DATE     DATE        not null,
    BRANCH_CODE  INTEGER,
    ID_CURRENCY  INTEGER,
    CUST_ID      INTEGER,
    UTIIZED_AMNT DECIMAL(15, 2),
    PAY_AMT      DECIMAL(15, 2),
    STATUS       CHAR(1),
    constraint IXU_CP_116
        primary key (VALUER_DT, ACCOUNT_NO, AGR_CODE, TRX_DATE)
);

