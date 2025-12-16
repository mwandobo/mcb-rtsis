create table CGN_PLAN_EMPL_IDS
(
    EMPL_ID      CHAR(8)     not null,
    EMPL_PLAN_ID VARCHAR(10) not null,
    constraint PK_EMPL_PLAN_ID
        primary key (EMPL_PLAN_ID, EMPL_ID)
);

