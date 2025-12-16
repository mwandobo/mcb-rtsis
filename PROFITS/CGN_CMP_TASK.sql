create table CGN_CMP_TASK
(
    CMP_TASK_ID       SMALLINT     not null,
    CMP_TASK_SN       SMALLINT     not null,
    DATE_FROM         DATE,
    DATE_TO           DATE,
    BUDGET_OWNER      CHAR(8)      not null,
    PLANNED_BUDGET    DECIMAL(15)  not null,
    PROPOSED_BUDGET   DECIMAL(15)  not null,
    ACTUAL_SPENDING   DECIMAL(15)  not null,
    COMMENTS          VARCHAR(80),
    ENTRY_STATUS      CHAR(1)      not null,
    TMSTAMP           TIMESTAMP(6) not null,
    FK_MARKET_PLAN_ID CHAR(10)     not null,
    FK_MARKET_PLAN_SN INTEGER      not null,
    FK_MKT_COST_ID    SMALLINT     not null,
    FK_MKT_PHASE_ID   SMALLINT     not null,
    FK_CAMPAIGN_ID    SMALLINT     not null,
    FK_GH_TASK        CHAR(5),
    FK_GD_TASK        INTEGER,
    FK_CURRENCY_ID    INTEGER,
    FK_COST_VIEW_SN   DECIMAL(10),
    constraint PK_CAMPAIGN_TASK
        primary key (FK_MARKET_PLAN_ID, FK_MARKET_PLAN_SN, FK_MKT_COST_ID, FK_MKT_PHASE_ID, FK_CAMPAIGN_ID, CMP_TASK_ID)
);

