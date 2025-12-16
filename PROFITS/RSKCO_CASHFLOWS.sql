create table RSKCO_CASHFLOWS
(
    REFERENCE_ID          CHAR(30) not null,
    PERIOD
    SMALLINT
    not null,
    LEG                   SMALLINT not null,
    FINAL_AMOUNT_DECIMALS SMALLINT,
    INTEREST_DECIMALS     SMALLINT,
    NOTIONAL_FLOW_DEC     SMALLINT,
    INTEREST_AMN_DEC      SMALLINT,
    NOTIONAL_AMN_DEC      SMALLINT,
    BASIS                 SMALLINT,
    INTEREST              DECIMAL(10),
    FINAL_AMOUNT          DECIMAL(15),
    NOTIONAL_FLOW         DECIMAL(15),
    INTEREST_AMOUNT       DECIMAL(15),
    NOTIONAL_AMOUNT       DECIMAL(15),
    START_DATE            DATE,
    PRFT_EXTRACTION_DT    DATE,
    END_DATE              DATE,
    PAYMENT_DATE          DATE,
    RESET_DATE            DATE,
    FINALIZED             CHAR(1),
    CURRENCY              CHAR(3),
    DAY_COUNT             CHAR(6),
    PRFT_ROUTINE          CHAR(20),
    constraint IXU_LNS_053
        primary key (REFERENCE_ID, PERIOD, LEG)
);

