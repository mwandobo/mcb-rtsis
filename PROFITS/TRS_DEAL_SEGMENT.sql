create table TRS_DEAL_SEGMENT
(
    SERIAL_NUMBER        DECIMAL(10) not null,
    FK_DEAL_NO           INTEGER     not null,
    START_DT             DATE,
    END_DT               DATE,
    INTEREST_PERC        DECIMAL(9, 6),
    COUPON_MATUR_INDEX   DECIMAL(8, 3),
    BOND_START_INDEX     DECIMAL(8, 3),
    TAX_PERC             DECIMAL(8, 4),
    ENTRY_STATUS         CHAR(1),
    ACCRUAL_AMOUNT       DECIMAL(15, 2),
    INTERBANK_TAX_AMOUNT DECIMAL(15, 2),
    CAPITAL_AMOUNT       DECIMAL(15, 2),
    INTEREST_AMOUNT      DECIMAL(15, 2),
    PAYMENT_DT           DATE,
    PAID_FLG             CHAR(1),
    INTEREST_PAYMENT     DECIMAL(15, 2),
    CUM_ACCRUAL_AMOUNT   DECIMAL(15, 2),
    HANDLING_FEE_PCTG    DECIMAL(8, 4)  default 0,
    HANDLING_FEE_AMOUNT  DECIMAL(15, 2) default 0,
    constraint PK_DEAL_SEGMENT
        primary key (FK_DEAL_NO, SERIAL_NUMBER)
);

