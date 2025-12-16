create table CP_DELTA_BRANCHES
(
    CODE         INTEGER,
    DELTA_BRANCH INTEGER
);

create unique index IXU_CP__020
    on CP_DELTA_BRANCHES (CODE);

