create table W_FACT_PROPERTY_TAX
(
    IP_LRNUM                    VARCHAR(150) not null,
    TR_ID                       VARCHAR(15)  not null,
    ROW_EFFECTIVE_DATE          DATE         not null,
    ROW_EXPIRATION_DATE         DATE,
    ROW_CURRENT_FLAG            DECIMAL(1),
    REALTY_ID                   DECIMAL(10),
    IP_TAX_PR                   VARCHAR(10),
    IP_TAX_PR_IND               VARCHAR(9),
    IP_TAX_DUE_DATE             DATE,
    IP_TAX_PAY_DATE             DATE,
    IP_TAX_AMOUNT               DECIMAL(19, 3),
    IP_TAX_AMOUNT_CURRENCY_ID   DECIMAL(5),
    IP_TAX_AMOUNT_CURRENCY_CODE VARCHAR(5),
    IP_TAX_LOAN_ACC             VARCHAR(20),
    constraint PK_W_FACT_PROPERTY_TAX
        primary key (IP_LRNUM, TR_ID, ROW_EFFECTIVE_DATE)
);

