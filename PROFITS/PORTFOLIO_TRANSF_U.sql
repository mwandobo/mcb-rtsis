create table PORTFOLIO_TRANSF_U
(
    CUST_ID        INTEGER not null,
    TRX_DATE       DATE    not null,
    BRANCH_CODE    INTEGER,
    PORTFOLIO_CODE INTEGER not null,
    PROCESSED_FLG  CHAR(1),
    constraint IXU_CIU_050
        primary key (TRX_DATE, CUST_ID)
);

