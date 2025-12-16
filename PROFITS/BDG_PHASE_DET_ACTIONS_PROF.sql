create table BDG_PHASE_DET_ACTIONS_PROF
(
    REMARKS    VARCHAR(200),
    TO_DATE    DATE,
    FROM_DATE  DATE,
    YEAR0      SMALLINT   not null,
    STAGE_ID   INTEGER    not null,
    PHASE_ID   CHAR(2)    not null,
    VERSION_ID INTEGER    not null,
    PERIOD_ID  VARCHAR(5) not null,
    ACTION_ID  VARCHAR(5) not null,
    FK_PROFILE CHAR(8)    not null,
    constraint IXU_BDG_PH_DET_ACT_PR
        primary key (FK_PROFILE, PERIOD_ID, VERSION_ID, PHASE_ID, STAGE_ID, YEAR0)
);

