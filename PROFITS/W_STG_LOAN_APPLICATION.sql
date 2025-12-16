create table W_STG_LOAN_APPLICATION
(
    APPL_ID                        VARCHAR(10),
    ACCOUNT_NUMBER                 CHAR(40),
    APPLICATION_TYPE               VARCHAR(100),
    APPROVAL_DATE                  DATE,
    SALES_PRICE                    DECIMAL(15, 2),
    RECEIVE_RENT_AMOUNT            DECIMAL(15, 2),
    UNIT_CODE                      VARCHAR(10),
    LOAN_OFFICER_ID                VARCHAR(10),
    LOAN_OFFICER_NAME              VARCHAR(40),
    CUST_ID                        VARCHAR(40),
    CUSTOMER_NAME                  VARCHAR(20),
    CUSTOMER_SURNAME               VARCHAR(120),
    APPROVED_LOAN_PRINCIPAL_AMOUNT DECIMAL(19, 3),
    LOAN_APPLICATION_DATE          DATE
);

