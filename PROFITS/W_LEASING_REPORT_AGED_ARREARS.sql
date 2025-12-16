create table W_LEASING_REPORT_AGED_ARREARS
(
    EOM_DATE               DATE         not null,
    PRODUCT                INTEGER,
    UNIT                   INTEGER,
    CURRENCY_ID            INTEGER,
    LOAN_STATUS            CHAR(1),
    ACC_STATUS             CHAR(1),
    LNS_UNIT               INTEGER,
    LNS_TYPE               INTEGER,
    LNS_SN                 INTEGER,
    CUSTOMER_NAME          VARCHAR(100),
    ACCOUNT_NUMBER         VARCHAR(100) not null,
    FINANCED_AMOUNT        DECIMAL(15, 2),
    NET_INVESTMENT         DECIMAL(15, 2),
    NEXT_DUE_DATE          DATE,
    OV_1_29                DECIMAL(15, 2),
    OV_30_89               DECIMAL(15, 2),
    OV_90_179              DECIMAL(15, 2),
    OV_180_PLUS            DECIMAL(15, 2),
    DAYS_IN_ARREARS        INTEGER,
    TOTAL_ARREARS          DECIMAL(15, 2),
    HEAD_OFFICE_SUPERVISOR VARCHAR(100),
    RELATIONSHIP_OFFICER   VARCHAR(100),
    BRANCH                 VARCHAR(100),
    constraint W_LEASING_REPORT_AGED_ARREARS_PK
        primary key (EOM_DATE, ACCOUNT_NUMBER)
);

