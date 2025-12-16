create table DEP_SCR_INTERFACE
(
    AVAILABILITY_DATE  DATE        not null,
    DEP_ACCOUNT_NUMBER DECIMAL(11) not null,
    SN                 SMALLINT    not null,
    TRACC_TYPE         SMALLINT,
    TRACC_UNIT         INTEGER,
    TRACC_SN           INTEGER,
    CUST_ID            INTEGER,
    BUY_COMMISION      DECIMAL(9, 6),
    SELL_COMMISION     DECIMAL(9, 6),
    AK_RATE_TABLE_NUM  DECIMAL(10),
    TRORDER_CODE       DECIMAL(12),
    AMOUNT             DECIMAL(15, 2),
    PROCESSED_TMSTAMP  DATE,
    CREATED_TMSTAMP    DATE,
    PROCESSED_DATE     DATE,
    CREATED_DATE       DATE,
    PROCESS_STATUS     CHAR(1),
    TRN_TYPE           CHAR(1),
    TRBOND_CODE        CHAR(15),
    constraint IXU_DEP_129
        primary key (AVAILABILITY_DATE, DEP_ACCOUNT_NUMBER, SN)
);

