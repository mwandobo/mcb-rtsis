-- auto-generated definition
create table GLI_TRX_EXTRACT
(
    FK_UNITCODETRXUNIT INTEGER  not null,
    FK_USRCODE         CHAR(8)  not null,
    LINE_NUM           SMALLINT not null,
    TRN_DATE           DATE     not null,
    TRN_SNUM           INTEGER  not null,
    ACC_C_DIGIT        SMALLINT,
    AMOUNT_SER_NO      SMALLINT,
    TUN_INTERNAL_SN    SMALLINT,
    AVAILABILITY_DAYS  SMALLINT,
    GL_RULE            INTEGER,
    TRX_CODE           INTEGER,
    ID_PRODUCT         INTEGER,
    FK1UNITCODE        INTEGER,
    FK0UNITCODE        INTEGER,
    CUST_ID            INTEGER,
    TRX_SN             INTEGER,
    BILL_SERIAL_NUM    DECIMAL(10),
    ACCOUNT_NUMBER     DECIMAL(13),
    FC_AMOUNT          DECIMAL(15, 2),
    DC_AMOUNT          DECIMAL(15, 2),
    AVAILABILITY_DATE  DATE,
    TRX_GL_TRN_DATE    DATE,
    TMSTAMP            TIMESTAMP(6),
    ENTRY_TYPE         CHAR(1),
    MIGRATED_FLAG      CHAR(1),
    LEVEL0             CHAR(1),
    THIRDPARTY_IND     CHAR(1),
    SUBSYSTEM          CHAR(2),
    ACH_BANK_CODE      CHAR(2),
    GLACC_ORIGIN       CHAR(2),
    ID_JUSTIFIC        CHAR(5),
    CURRENCY_SHORT_DES CHAR(5),
    FIELD_1_2_SEPERAT  CHAR(6),
    FIELD_3_4_SEPERAT  CHAR(6),
    TRX_USR            CHAR(8),
    CHEQUE_NUMBER      CHAR(10),
    CUSTOMER_ID_NO     CHAR(20),
    BILL_NUMBER        CHAR(20),
    FK_GLG_ACCOUNTACCO CHAR(21),
    TPP_FIELD_3        CHAR(30),
    TPP_FIELD_2        CHAR(30),
    TPP_FIELD_1        CHAR(30),
    TPP_FIELD_4        CHAR(30),
    ENTRY_COMMENTS     CHAR(40),
    EXTERNAL_GLACCOUNT CHAR(21),
    JUSTIFIC_DESCR     VARCHAR(40),
    PRF_ACCOUNT_NUMBER CHAR(40),
    PRF_ACC_CD         SMALLINT,
    constraint IXU_GLI_000
        primary key (FK_UNITCODETRXUNIT, FK_USRCODE, LINE_NUM, TRN_DATE, TRN_SNUM)
);

create unique index GLI_TRX_EXTRACT_INDEX1
    on GLI_TRX_EXTRACT (TRX_GL_TRN_DATE, FK1UNITCODE);

create unique index GLI_TRX_EXTRACT_INDEX2
    on GLI_TRX_EXTRACT (FK_GLG_ACCOUNTACCO);

create unique index IDX_GLI_TRX_HKEEP
    on GLI_TRX_EXTRACT (TRN_DATE);

