create table ISSU_PROD_CUST_U
(
    PRODUCT         INTEGER not null,
    CUSTOMER        INTEGER not null,
    CR_DEP_ACCOUNT  DECIMAL(11),
    DEFAULT_IND     SMALLINT,
    BANK_ACCOUNT_NO CHAR(40),
    constraint IXU_CIU_044
        primary key (CUSTOMER, PRODUCT)
);

