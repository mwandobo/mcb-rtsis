create table BDG_PHASE
(
    STAGE_ID           INTEGER not null,
    ID                 CHAR(2) not null,
    DESCRIPTION        VARCHAR(30),
    BASE_COMP_BDG_DATA CHAR(1),
    BASE_COMP_REV_DATA CHAR(1),
    BASE_COMP_ACT_DATA CHAR(1),
    TMSTMP             TIMESTAMP(6),
    STATUS             SMALLINT,
    constraint IXU_GL_028_PRIMARY_KEY
        primary key (ID, STAGE_ID)
);

