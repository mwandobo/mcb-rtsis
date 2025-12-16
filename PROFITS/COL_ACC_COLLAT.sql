create table COL_ACC_COLLAT
(
    ACC_CD          SMALLINT,
    COL_PRODUCT_ID  INTEGER,
    COLLATERAL_PERC DECIMAL(8, 4),
    ACC_SN          DECIMAL(13),
    TMSTAMP         TIMESTAMP(6),
    TRX_DATE        DATE,
    ENTRY_STATUS    CHAR(1)
);

create unique index PK_ACCCOLAT
    on COL_ACC_COLLAT (ACC_SN, COL_PRODUCT_ID);

