create table W_LEASING_REPORT_VAT_OUTPUT
(
    EOM_DATE                 DATE         not null,
    PRODUCT                  INTEGER,
    UNIT                     INTEGER,
    CURRENCY_ID              INTEGER,
    LOAN_STATUS              CHAR(1),
    ACC_STATUS               CHAR(1),
    ACCOUNT_NUMBER           VARCHAR(100) not null,
    CUST_ID                  INTEGER,
    TIN_NUMBER               VARCHAR(250),
    CUSTOMER_NAME            VARCHAR(250),
    INVOICE_PARTICULARS      VARCHAR(250),
    INVOICE_OR_CREDIT_NUMBER VARCHAR(250) not null,
    FDN                      VARCHAR(100),
    ISSUE_DATE               VARCHAR(30),
    VALUE_DATE               VARCHAR(30),
    NET_AMOUNT               DECIMAL(15, 2),
    VAT_AMOUNT               DECIMAL(15, 2),
    GROSS_AMOUNT             DECIMAL(15, 2),
    VAT_CATEGORY             VARCHAR(100),
    VAT_RATE                 INTEGER,
    CURRENCY                 VARCHAR(100),
    INVOICE_NUMBER           VARCHAR(250),
    INVOICE_FDN_NUMBER       VARCHAR(100),
    IVOICE_DATE              DATE,
    INVOICE_TYPE             CHAR(1)      not null,
    constraint W_LEASING_REPORT_VAT_OUTPUT_PK
        primary key (EOM_DATE, ACCOUNT_NUMBER, INVOICE_TYPE, INVOICE_OR_CREDIT_NUMBER)
);

