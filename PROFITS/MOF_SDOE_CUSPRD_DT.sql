create table MOF_SDOE_CUSPRD_DT
(
    RECORD_TYPE    CHAR(2),
    BATCH_ID       CHAR(12) not null,
    FILE_ID        SMALLINT not null,
    UNIQUE_REF_ID  CHAR(12) not null,
    CUSTOMER_CD    CHAR(40) not null,
    ACCOUNT_NUMBER CHAR(27) not null,
    RESPON_BANK_CD CHAR(3),
    TARGET_BANK_CD CHAR(3),
    CCY_ISO_CODE   CHAR(3),
    ACC_IN_DATE    CHAR(10),
    ACC_OUT_DATE   CHAR(10),
    BENEF_SN       CHAR(2),
    RELATION_TYPE  CHAR(2),
    ERROR_FIELD    CHAR(2),
    ERROR_CODE     CHAR(2),
    constraint CUSPRD_DT
        primary key (FILE_ID, ACCOUNT_NUMBER, CUSTOMER_CD, UNIQUE_REF_ID, BATCH_ID)
);

