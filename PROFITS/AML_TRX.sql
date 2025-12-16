create table AML_TRX
(
    CUST_TYPE          CHAR(1),
    CITY               CHAR(30),
    ZIP_CODE           CHAR(10),
    REGION             VARCHAR(20),
    ADDRESS_1          VARCHAR(40),
    ADDRESS_2          VARCHAR(40),
    ID_NO              CHAR(20)    not null,
    AFM_NO             CHAR(20)    not null,
    FIRST_NAME         CHAR(20),
    SURNAME            CHAR(70)    not null,
    TRX_DATE           DATE        not null,
    TRX_UNIT           INTEGER     not null,
    TRX_USR            CHAR(8)     not null,
    TRX_USR_SN         INTEGER     not null,
    TUN_INTERNAL_SN    SMALLINT    not null,
    GLI_LINE_NUM       SMALLINT    not null,
    BANK_DRAFT_NUM     CHAR(10)    not null,
    BANK_DRAFT_ACCOUNT DECIMAL(11) not null,
    ID_TRANSACT        INTEGER,
    ID_JUSTIFIC        INTEGER,
    TRX_TYPE           CHAR(1),
    TRX_CATEGORY       CHAR(1),
    PRF_ACCOUNT_NUMBER CHAR(40),
    PRF_ACCOUNT_CD     SMALLINT,
    CUSTOMER_ID        INTEGER,
    C_DIGIT            SMALLINT,
    AMOUNT             DECIMAL(15, 2),
    ENTRY_TYPE         CHAR(1),
    ID_CURRENCY        INTEGER,
    PRFT_REF_NO        CHAR(16)    not null,
    BANK_ORDER_NUMBER  INTEGER     not null,
    UNIT_ORDER_NUMBER  INTEGER     not null,
    SUBSYSTEM          CHAR(2),
    IBAN_NO            CHAR(37),
    GLI_REMARKS        CHAR(50),
    TRX_COMMENTS       VARCHAR(40),
    CUST_ID_TYPE       INTEGER,
    LNS_SN             INTEGER,
    DEP_SN             INTEGER,
    constraint PK_AML_TRX
        primary key (TRX_UNIT, TRX_DATE, TRX_USR, TRX_USR_SN, TUN_INTERNAL_SN, GLI_LINE_NUM)
);

create unique index SK1_AML_TRX
    on AML_TRX (CUST_TYPE, CUSTOMER_ID);

