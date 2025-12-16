create table LOAN_ACC_COLLAT
(
    ACC_UNIT        SMALLINT     not null,
    ACC_TYPE        SMALLINT     not null,
    ACC_SN          INTEGER      not null,
    ACC_CD          SMALLINT,
    TMSTAMP         TIMESTAMP(6) not null,
    TRX_DATE        DATE,
    COL_PRODUCT_ID  INTEGER,
    COLLATERAL_PERC DECIMAL(8, 4),
    ENTRY_STATUS    CHAR(1),
    constraint PKACCCOL
        primary key (TMSTAMP, ACC_SN, ACC_TYPE, ACC_UNIT)
);

