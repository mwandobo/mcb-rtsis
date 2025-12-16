create table MG_COLL_ACCOUNT
(
    FILE_NAME         CHAR(50) not null,
    SERIAL_NO         INTEGER  not null,
    COLLATERAL_SN     CHAR(40),
    COVERED_ACCOUNT   CHAR(40),
    COV_ACC_SYSTEM    SMALLINT,
    COL_EST_VALUE_AMN DECIMAL(15, 2),
    COL_EST_INSUR_AMN DECIMAL(15, 2),
    COL_VALUE_CBANK   DECIMAL(15, 2),
    COL_EXPIRY_DT     DATE,
    ROW_STATUS        CHAR(1),
    FILE_DETAIL_ID    CHAR(2),
    constraint PK_MG_COLL_ACC
        primary key (SERIAL_NO, FILE_NAME)
);

