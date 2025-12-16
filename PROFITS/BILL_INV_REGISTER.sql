create table BILL_INV_REGISTER
(
    BINV_SERIAL_NUM    DECIMAL(10)    not null
        constraint IXU_BIL_31
            primary key,
    BINV_REF_NO        CHAR(15)       not null,
    BINV_TOTBILL_COUNT INTEGER        not null,
    BINV_TOTBILL_AMNT  DECIMAL(15, 2) not null,
    BINV_INS_DATE      DATE           not null,
    BINV_FINAL_DATE    DATE           not null,
    BINV_CARRIER_TITLE CHAR(70),
    BINV_CARRIER_NAME  CHAR(30),
    BINV_CARRIER_ID    CHAR(10),
    BINV_CARRIER_NOTES CHAR(100),
    BINV_BILLANAL_FLAG CHAR(1),
    BINV_ENTRY_STATUS  CHAR(1)        not null,
    EXPACC_NUMBER      CHAR(40),
    EXPACC_CD          SMALLINT,
    EXPACC_PRFSYS      SMALLINT,
    EXPACC_INTERDATE   DATE,
    COMM_DISCOUNT      DECIMAL(8, 4),
    EXP_DISCOUNT       DECIMAL(8, 4),
    TMSTAMP            TIMESTAMP(6),
    FK_USRCODE         CHAR(8),
    FK_CUSTOMERCUST_ID INTEGER,
    FK_UNITCODE        INTEGER,
    FK_PRODUCT_ID      INTEGER,
    BINV_TYPE_FLAG     CHAR(1)
);

create unique index IXN_BIL_33
    on BILL_INV_REGISTER (FK_PRODUCT_ID);

create unique index IXN_BIL_34
    on BILL_INV_REGISTER (FK_USRCODE);

create unique index IXN_BIL_35
    on BILL_INV_REGISTER (FK_UNITCODE);

create unique index IXN_BIL_36
    on BILL_INV_REGISTER (FK_CUSTOMERCUST_ID);

