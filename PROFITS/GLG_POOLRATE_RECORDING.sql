create table GLG_POOLRATE_RECORDING
(
    TRX_DATE           DATE     not null,
    TRAN_ID            CHAR(6)  not null,
    FK_GLG_ACCOUNTACC0 CHAR(21) not null,
    FK_CURRENCYID_CURR INTEGER  not null,
    FK_UNITCODE        INTEGER  not null,
    PERCENTAGE         DECIMAL(9, 6),
    DAYS               DECIMAL(15),
    BASEDAYS           SMALLINT,
    CUMULATIVE_BALANCE DECIMAL(18, 2),
    FC_AMOUNT          DECIMAL(18, 2),
    TIMESTMP           TIMESTAMP(6),
    REMARKS            VARCHAR(100),
    constraint IXU_GLG_POOLRATE_R
        primary key (TRX_DATE, TRAN_ID, FK_GLG_ACCOUNTACC0, FK_CURRENCYID_CURR, FK_UNITCODE)
);

