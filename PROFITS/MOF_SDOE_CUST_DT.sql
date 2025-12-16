create table MOF_SDOE_CUST_DT
(
    RECORD_TYPE    CHAR(2),
    BATCH_ID       CHAR(12) not null,
    FILE_ID        SMALLINT not null,
    UNIQUE_REF_ID  CHAR(12) not null,
    CUSTOMER_CD    CHAR(40) not null,
    RESPON_BANK_CD CHAR(3),
    TARGET_BANK_CD CHAR(3),
    TAX_ID         CHAR(9),
    CUSTOMER_TITLE CHAR(70),
    FIRST_NAME     CHAR(20),
    FATHERNAME     CHAR(20),
    MOTHERNAME     CHAR(20),
    BIRTH_DATE     CHAR(10),
    IDDOC_TYPE     CHAR(2),
    IDDOC_NUM      CHAR(20),
    PROFESS        CHAR(20),
    ADDRESS        CHAR(25),
    REGION         CHAR(10),
    ZIP_CODE       CHAR(5),
    ERROR_FIELD    CHAR(2),
    ERROR_CODE     CHAR(2),
    constraint PK_CUST_DT
        primary key (FILE_ID, CUSTOMER_CD, UNIQUE_REF_ID, BATCH_ID)
);

