create table LNS_CRD_CUST_SUB_DATA
(
    REPORTING_DATE   DATE        not null,
    LOAN_CODE        VARCHAR(40) not null,
    CUST_ID          INTEGER     not null,
    CLIENT_ROLE      VARCHAR(128),
    COLLATERAL_VALUE DECIMAL(15, 2),
    constraint CRDLO02
        primary key (REPORTING_DATE, LOAN_CODE, CUST_ID)
);

