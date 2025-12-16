create table LNS_MULTI_TRX_GUARANT
(
    APPLICATION_ID   CHAR(15)   not null,
    CUST_ID          DECIMAL(7) not null,
    GUARANTEE_AMOUNT DECIMAL(15, 2),
    AGR_PERC_AMOUNT  DECIMAL(15, 2),
    REMOVAL_DT       DATE,
    GUARANTEE_DT     DATE,
    TMSTAMP          TIMESTAMP(6),
    constraint I0001014
        primary key (CUST_ID, APPLICATION_ID)
);

