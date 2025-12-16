create table CPM_LOANS_PAY_ON_REQ
(
    LIQUIDATOR        SMALLINT not null,
    ACC_UNIT          SMALLINT not null,
    ACC_TYPE          SMALLINT not null,
    ACC_SN            INTEGER  not null,
    ACC_CD            SMALLINT not null,
    BIG_CUST_CODE     INTEGER  not null,
    SERIAL_NO         SMALLINT not null,
    CREATION_DT       DATE     not null,
    ENTRY_STATUS      CHAR(1),
    REFEED_STATUS     CHAR(1),
    ERROR_DESCRIPTION VARCHAR(100),
    TMSTAMP           TIMESTAMP(6),
    constraint ICPM0132
        primary key (LIQUIDATOR, ACC_UNIT, ACC_TYPE, ACC_SN, ACC_CD, BIG_CUST_CODE, SERIAL_NO, CREATION_DT)
);

