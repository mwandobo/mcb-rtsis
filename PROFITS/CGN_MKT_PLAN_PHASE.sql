create table CGN_MKT_PLAN_PHASE
(
    MKT_PHASE_ID      SMALLINT     not null,
    MKT_PHASE_SN      SMALLINT     not null,
    PHASE_TITLE       VARCHAR(50),
    DATE_FROM         DATE         not null,
    DATE_TO           DATE         not null,
    PLANNED_BUDGET    DECIMAL(15)  not null,
    ACTUAL_SPENDING   DECIMAL(15)  not null,
    TMSTAMP           TIMESTAMP(6) not null,
    FK_MARKET_PLAN_ID CHAR(10)     not null,
    FK_MARKET_PLAN_SN INTEGER      not null,
    FK_CURRENCY_ID    INTEGER,
    constraint PK_MKT_PLAN_PHASE
        primary key (FK_MARKET_PLAN_ID, FK_MARKET_PLAN_SN, MKT_PHASE_ID)
);

