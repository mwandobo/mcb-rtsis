create table PAT_SCENARIO_SCHEDULER
(
    ID                         INTEGER   not null
        constraint PATSCHPK
            primary key,
    DESCRIPTION                CHAR(240) not null,
    CURRENT_SCENARIO           INTEGER   not null,
    NEXT_SCENARIO_WHEN_OK      INTEGER   not null,
    RETRY_LIMIT                SMALLINT  not null,
    NEXT_SCENARION_WHEN_FAILED INTEGER   not null,
    SN                         INTEGER   not null,
    FK_PAT_TEST_MANID          CHAR(10),
    STATUS                     CHAR(1)   not null,
    LOOP_INDEX_FROM            SMALLINT  not null,
    LOOP_INDEX_TO              SMALLINT
);

create unique index PATSCHI1
    on PAT_SCENARIO_SCHEDULER (FK_PAT_TEST_MANID);

