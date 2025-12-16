create table CGN_MKT_PLAN_GOAL
(
    MKT_PHASE_ID      SMALLINT     not null,
    MKT_GOAL_ID       SMALLINT     not null,
    MKT_GOAL_SN       SMALLINT     not null,
    GOAL_TITLE        VARCHAR(50),
    CUSTOMER_COUNT    DECIMAL(10),
    ACCOUNT_COUNT     DECIMAL(10),
    COMMENTS          LONG VARCHAR(32700),
    TMSTAMP           TIMESTAMP(6) not null,
    FK_MARKET_PLAN_ID CHAR(10)     not null,
    FK_MARKET_PLAN_SN INTEGER      not null,
    FK_KPI_VIEW_SN    DECIMAL(10),
    constraint PK_MKT_PLAN_GOAL
        primary key (FK_MARKET_PLAN_ID, FK_MARKET_PLAN_SN, MKT_PHASE_ID, MKT_GOAL_ID)
);

