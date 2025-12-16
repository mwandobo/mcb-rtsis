create table TEMP_TEIRESIA_BL_F
(
    LINE_SN            INTEGER not null,
    BANK_CODE          CHAR(3),
    INFO_TYPE          CHAR(1),
    FILE_DATE          CHAR(8) not null,
    RECORD_COUNT       INTEGER,
    FK_UNITCODE        SMALLINT,
    LOAN_PRIMARY_KEY   CHAR(16),
    TEIRESIAS_DATE     CHAR(8),
    LOAN_TYPE          CHAR(1),
    CUSTOMER_INDICATOR CHAR(1),
    AFM_NO             CHAR(9),
    CUST_ID_NO         CHAR(9),
    CUST_BIRTH_DATE    CHAR(8),
    CUST_SEX           CHAR(1),
    CUST_SURNAME       CHAR(24),
    CUST_FIRST_NAME    CHAR(6),
    CUST_FATHER_NAME   CHAR(3),
    CUST_ADDRESS_1     CHAR(20),
    CUST_ADDRESS_NO    CHAR(4),
    CUST_CITY          CHAR(20),
    CUST_ZIP_CODE      CHAR(5),
    LOAN_INDICATOR     CHAR(1),
    LOAN_IND_TRX_DATE  CHAR(8),
    constraint PKTEIRBL
        primary key (LINE_SN, FILE_DATE)
);

