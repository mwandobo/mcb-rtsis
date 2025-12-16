create table GLG_TEMP_ACCOUNTS
(
    SN                    DECIMAL(8)  not null,
    GLACCOUNT             VARCHAR(21) not null,
    FK_ACCOUNTING_RULE    DECIMAL(5),
    FK_ACC_RULE_DESCR     VARCHAR(40),
    FK_PRODUCTID_PRODU    DECIMAL(5),
    FK_PRODUCT_DESCR      VARCHAR(40),
    FK_ORIGIN_ID          CHAR(2),
    FK_ORIGIN_TYPE        CHAR(1),
    FK_CHARGE_CODE        DECIMAL(5),
    FK_GLG_ENTEP_CTL      CHAR(1),
    FK_LG_BATCH_PARAMETER DECIMAL(1),
    WHEREIS               VARCHAR(3000),
    TRX_USR               CHAR(8)     not null,
    TRX_DATE              DATE        not null,
    constraint PK_GLG_TEMP_ACCOUNTS
        primary key (TRX_DATE, TRX_USR, SN, GLACCOUNT)
);

