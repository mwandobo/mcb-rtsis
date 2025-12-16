create table W_LEASING_REPORT_VAT_INPUT
(
    EOM_DATE            DATE         not null,
    PRODUCT             INTEGER,
    UNIT                INTEGER,
    CURRENCY_ID         INTEGER,
    LOAN_STATUS         CHAR(1),
    ACC_STATUS          CHAR(1),
    ACCOUNT_NUMBER      VARCHAR(100) not null,
    CUST_ID             INTEGER      not null,
    CUSTOMER_NAME       VARCHAR(100),
    SUPPLIER_TIN_NUMBER VARCHAR(20),
    ASSESSMENT_NUMBER   VARCHAR(250),
    SUPPLIER_NUMBER     INTEGER,
    SUPPLIER_NAME       VARCHAR(80),
    INVOICE_PARTICULARS VARCHAR(250),
    INVOICE_NUMBER      VARCHAR(250),
    FDN                 VARCHAR(100),
    ISSUE_DATE          DATE,
    VALUE_DATE          DATE,
    NET_AMOUNT          DECIMAL(15, 2),
    VAT_AMOUNT          DECIMAL(15, 2),
    GROSS_AMOUNT        DECIMAL(15, 2),
    VAT_CATEGORY        VARCHAR(100),
    VAT_RATE            INTEGER,
    CURRENCY            VARCHAR(100)
);

