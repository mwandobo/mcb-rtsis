create table CGN_CAMPAIGN_PLAN
(
    CAMPAIGN_ID        SMALLINT     not null,
    CAMPAIGN_SN        SMALLINT     not null,
    CAMPAIGN_TITLE     VARCHAR(50),
    CMP_PLAN_OWNER     CHAR(8)      not null,
    DATE_FROM          DATE         not null,
    DATE_TO            DATE         not null,
    ACTUAL_SPENDING    DECIMAL(15)  not null,
    MKT_CUSTOMER_COUNT DECIMAL(10),
    MKT_ACCOUNT_COUNT  DECIMAL(10),
    KPI_CUSTOMER_COUNT DECIMAL(10),
    KPI_ACCOUNT_COUNT  DECIMAL(10),
    ENTRY_STATUS       CHAR(1)      not null,
    TMSTAMP            TIMESTAMP(6) not null,
    FK_MARKET_PLAN_ID  CHAR(10)     not null,
    FK_MARKET_PLAN_SN  INTEGER      not null,
    FK_MKT_PHASE_ID    SMALLINT     not null,
    FK_MKT_COST_ID     SMALLINT     not null,
    FK_KPI_VIEW_SN     DECIMAL(10),
    FK_CUST_VIEW_SN    DECIMAL(10),
    constraint PK_CAMPAIGN_PLAN
        primary key (FK_MARKET_PLAN_ID, FK_MARKET_PLAN_SN, FK_MKT_COST_ID, FK_MKT_PHASE_ID, CAMPAIGN_ID)
);

