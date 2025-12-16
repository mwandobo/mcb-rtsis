create table BDG_PHASE_DET_ACTIONS
(
    REMARKS    VARCHAR(200),
    TO_DATE    DATE,
    FROM_DATE  DATE,
    ACTION_ID  VARCHAR(5) not null,
    PERIOD_ID  VARCHAR(2) not null,
    VERSION_ID INTEGER    not null,
    PHASE_ID   CHAR(2)    not null,
    STAGE_ID   INTEGER    not null,
    YEAR0      SMALLINT   not null,
    constraint IXU_BDG_PH_DET_ACT
        primary key (YEAR0, STAGE_ID, PHASE_ID, VERSION_ID, PERIOD_ID)
);

