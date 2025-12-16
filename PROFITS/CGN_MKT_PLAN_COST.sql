create table CGN_MKT_PLAN_COST
(
    MKT_PHASE_ID      SMALLINT     not null,
    MKT_COST_ID       SMALLINT     not null,
    MKT_COST_SN       SMALLINT     not null,
    EXPENSE_TITLE     VARCHAR(50),
    APPROVAL_STATUS   CHAR(1),
    DATE_FROM         DATE         not null,
    DATE_TO           DATE         not null,
    BUDGET_OWNER      CHAR(8)      not null,
    PLANNED_BUDGET    DECIMAL(15)  not null,
    PROPOSED_BUDGET   DECIMAL(15)  not null,
    ACTUAL_SPENDING   DECIMAL(15)  not null,
    ENTRY_STATUS      CHAR(1)      not null,
    TMSTAMP           TIMESTAMP(6) not null,
    FK_MARKET_PLAN_ID CHAR(10)     not null,
    FK_MARKET_PLAN_SN INTEGER      not null,
    FK_CURRENCY_ID    INTEGER,
    FK_COST_VIEW_SN   DECIMAL(10),
    constraint PK_MKT_PLAN_COST
        primary key (FK_MARKET_PLAN_ID, FK_MARKET_PLAN_SN, MKT_COST_ID, MKT_PHASE_ID)
);

