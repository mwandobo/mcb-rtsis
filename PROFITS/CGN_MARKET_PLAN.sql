create table CGN_MARKET_PLAN
(
    MKT_PLAN_ID    CHAR(10)     not null,
    MKT_PLAN_SN    INTEGER      not null,
    PLAN_TITLE     VARCHAR(50),
    MKT_PLAN_OWNER CHAR(8)      not null,
    ENTRY_STATUS   CHAR(1)      not null,
    TMSTAMP        TIMESTAMP(6) not null,
    constraint PK_MKT_PLAN
        primary key (MKT_PLAN_ID, MKT_PLAN_SN)
);

