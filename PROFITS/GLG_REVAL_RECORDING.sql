create table GLG_REVAL_RECORDING
(
    TRX_DATE           DATE     not null,
    TRAN_ID            CHAR(6)  not null,
    FK_GLG_ACCOUNTACC0 CHAR(21) not null,
    FK_CURRENCYID_CURR INTEGER  not null,
    FK_UNITCODE        INTEGER  not null,
    FIXING_RATE        DECIMAL(12, 6),
    CUMULATIVE_BALANCE DECIMAL(18, 2),
    LC_AMOUNT_BEFORE   DECIMAL(18, 2),
    FK_GLG_ACCOUNTPL   CHAR(21),
    LC_AMOUNT          DECIMAL(18, 2),
    TIMESTMP           TIMESTAMP(6),
    REMARKS            VARCHAR(100),
    constraint IXU_GLG_REVAL_R
        primary key (TRX_DATE, TRAN_ID, FK_GLG_ACCOUNTACC0, FK_CURRENCYID_CURR, FK_UNITCODE)
);

