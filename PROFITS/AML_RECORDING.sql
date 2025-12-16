create table AML_RECORDING
(
    PRFT_SYSTEM           SMALLINT,
    TUN_INTERNAL_SN       SMALLINT,
    CHANNEL_ID            INTEGER,
    TRX_UNIT              INTEGER,
    ADDRESS_COUNTRY_ID    INTEGER,
    TAX_OFFICE_ID         INTEGER,
    ID_JUSTIFIC           INTEGER,
    ID_TRANSACT           INTEGER,
    SOURCE_CURRENCY       INTEGER,
    TARGET_CURRENCY       INTEGER,
    CUST_ID               INTEGER,
    TRX_USR_SN            INTEGER,
    FIXING_RATE           DECIMAL(12, 6),
    LOCAL_TARGET_AMOUNT   DECIMAL(15, 2),
    LOCAL_SOURCE_AMOUNT   DECIMAL(15, 2),
    TARGET_AMOUNT         DECIMAL(15, 2),
    SOURCE_AMOUNT         DECIMAL(15, 2),
    TRX_DATE              DATE,
    DATE_OF_BIRTH         DATE,
    TMSTAMP               TIMESTAMP(6),
    TRN_TYPE              CHAR(1),
    TRX_USR               CHAR(8),
    TELEPHONE             CHAR(15),
    MOBILE_PHONE          CHAR(15),
    FIRST_NAME            CHAR(20),
    TRX_DESCR             CHAR(40),
    JUST_DESCR            CHAR(40),
    SOURCE_ACCOUNT_NUMBER CHAR(40),
    TARGET_ACCOUNT_NUMBER CHAR(40),
    SURNAME               CHAR(70),
    FATHER_NAME           VARCHAR(20)
);

create unique index IX_AML_RECORDING_CUSTID
    on AML_RECORDING (CUST_ID);

create unique index PK_AML_RECORDING
    on AML_RECORDING (TRX_UNIT, TRX_DATE, TRX_USR, TRX_USR_SN, TUN_INTERNAL_SN, TMSTAMP);

