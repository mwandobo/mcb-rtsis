create table OVERDUE_COM
(
    TRX_DATE        DATE     not null,
    ACC_UNIT        INTEGER  not null,
    ACC_TYPE        SMALLINT not null,
    ACC_SN          INTEGER  not null,
    CUST_ID         INTEGER,
    PRODUCT_ID      INTEGER,
    PROCESS_FLG     CHAR(1),
    ERROR_MESSAGE   CHAR(40),
    DEBIT_DATE      DATE,
    DEP_ACC_NUM     DECIMAL(11),
    PROFITS_ACC_NUM CHAR(40),
    constraint IXU_DEP_105
        primary key (TRX_DATE, ACC_UNIT, ACC_TYPE, ACC_SN)
);

