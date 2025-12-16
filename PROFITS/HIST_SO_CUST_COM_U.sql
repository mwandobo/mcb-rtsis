create table HIST_SO_CUST_COM_U
(
    DEP_PROFITS_ACC    CHAR(40),
    CUST_ID            INTEGER        not null,
    JUSTIFIC_ID        INTEGER        not null,
    ACTIV_DATE         DATE           not null,
    ACTION_ENTRY_DESCR VARCHAR(255),
    AMOUNT             DECIMAL(15, 2) not null,
    PAYMENT_DATE       DATE           not null,
    ATTEMPTS_COUNT     INTEGER        not null,
    ENTRY_STATUS       CHAR(1),
    constraint IXU_CIU_042
        primary key (ACTIV_DATE, CUST_ID)
);

