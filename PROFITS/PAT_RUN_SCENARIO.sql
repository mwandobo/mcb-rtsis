create table PAT_RUN_SCENARIO
(
    ID                 INTEGER      not null
        constraint PATSSPK1
            primary key,
    NAME               CHAR(15)     not null,
    CREATION_TIMESTAMP TIMESTAMP(6) not null,
    TYPE0              CHAR(2)      not null,
    DESCRIPTION        CHAR(240),
    OUTPUT_DIRECTORY   CHAR(240),
    STATUS             CHAR(1)      not null,
    RUN_COUNT          INTEGER      not null,
    EXECUTABLE         CHAR(20),
    ARG_HINT           CHAR(10),
    PROJECT            CHAR(20),
    SCHEDULER          CHAR(1),
    EXE_PATH           CHAR(200),
    PARAMETER6         CHAR(20)
);

