create table BDG_PHASE_DET
(
    REMARKS           CHAR(200),
    TMSTMP            TIMESTAMP(6),
    STATUS            SMALLINT,
    DEACTIVATION_DATE DATE,
    CREATION_DATE     DATE,
    VERSION_ID        INTEGER  not null,
    PHASE_ID          CHAR(2)  not null,
    YEAR0             SMALLINT not null,
    STAGE_ID          INTEGER  not null,
    constraint IXU_BDG_PH_DET
        primary key (STAGE_ID, YEAR0, PHASE_ID, VERSION_ID)
);

