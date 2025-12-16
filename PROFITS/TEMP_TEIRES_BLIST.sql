create table TEMP_TEIRES_BLIST
(
    FILE_DATE          CHAR(8) not null,
    LINE_SN            INTEGER not null,
    BANK_CODE          SMALLINT,
    FK_UNITCODE        INTEGER,
    RECORD_COUNT       INTEGER,
    LOAN_TYPE          CHAR(1),
    CUSTOMER_INDICATOR CHAR(1),
    PAY_SET_IND        CHAR(1),
    INFO_TYPE          CHAR(1),
    CUST_SEX           CHAR(1),
    CUST_FATHER_NAME   CHAR(3),
    CUST_ADDRESS_NO    CHAR(4),
    CUST_ZIP_CODE      CHAR(5),
    CUST_FIRST_NAME    CHAR(6),
    LAST_TRX_DT        CHAR(8),
    PAY_SET_DATE       CHAR(8),
    CUST_BIRTH_DATE    CHAR(8),
    CUST_ID_NO         CHAR(9),
    AFM_NO             CHAR(9),
    LOAN_PRIMARY_KEY   CHAR(16),
    CUST_ADDRESS_1     CHAR(20),
    CUST_CITY          CHAR(20),
    CUST_SURNAME       CHAR(24),
    ACCOUNT_BALANCE    DECIMAL(15, 2),
    constraint IXU_TEM_010
        primary key (FILE_DATE, LINE_SN)
);

