create table GLG_ATM_FEES
(
    LSN            DECIMAL(15) not null,
    LMEMACT        CHAR(1)     not null,
    TRX_DATE       DATE        not null,
    TRX_TIMESTAMP  TIMESTAMP(6),
    LCYCLE         CHAR(2)     not null,
    LTXNCODE       CHAR(2)     not null,
    NUMBER_TRX     DECIMAL(20),
    NET_AMOUNT     DECIMAL(15, 2),
    FEE_AMOUNT     DECIMAL(15, 2),
    AMOUNT         DECIMAL(15, 2),
    LCURRENCY      DECIMAL(5),
    STATUS         CHAR(1),
    FROMDATE       DATE,
    TODATE         DATE,
    PROC_TIMESTAMP TIMESTAMP(6),
    constraint R_CUST_ACC_CH_PK
        primary key (LSN, LMEMACT, TRX_DATE, LTXNCODE, LCYCLE)
);

