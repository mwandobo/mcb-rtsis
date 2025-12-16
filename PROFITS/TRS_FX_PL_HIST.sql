create table TRS_FX_PL_HIST
(
    DEAL_NO           INTEGER        not null,
    UPDATE_DATE       DATE           not null,
    UNREALIZED_PL_LCY DECIMAL(15, 2),
    BUY_RATE          DECIMAL(12, 6),
    SELL_RATE         DECIMAL(12, 6) not null,
    TMSTAMP           TIMESTAMP(6),
    constraint PK_FX_PL_HIST
        primary key (UPDATE_DATE, DEAL_NO)
);

