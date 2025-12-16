create table CP_DEBTS
(
    CUST_ID         INTEGER     not null,
    TRX_DATE        DATE        not null,
    CP_AGREEMENT_NO DECIMAL(10) not null,
    ID_CURRENCY     INTEGER,
    BRANCH_CODE     INTEGER,
    ACCOUNT_NO      DECIMAL(11),
    INTERNAL_SN     DECIMAL(13),
    PAY_AMT         DECIMAL(15, 2),
    UTILIZED_AMNT   DECIMAL(15, 2),
    VALUER_DT       DATE,
    ENTRY_STATUS    CHAR(1),
    constraint IXU_CP_080
        primary key (CUST_ID, TRX_DATE, CP_AGREEMENT_NO)
);

