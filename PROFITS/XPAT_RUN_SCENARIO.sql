create table XPAT_RUN_SCENARIO
(
    ID                 INTEGER      not null
        constraint PATXSSPK
            primary key,
    NAME0              CHAR(15)     not null,
    CREATION_TIMESTAMP TIMESTAMP(6) not null,
    TYPE0              CHAR(2)      not null,
    DESCRIPTION        CHAR(240),
    INPUT_DIRECTORY    CHAR(240),
    STATUS             CHAR(1)      not null,
    RUN_COUNT          INTEGER      not null,
    EXECUTABLE         CHAR(20),
    ARG_HINT           CHAR(10),
    PROJECT            CHAR(20),
    SCHEDULER          CHAR(1),
    EXE_PATH           CHAR(200),
    PARAMETER6         CHAR(20)
);

comment on column XPAT_RUN_SCENARIO.TYPE0 is 'SETUP or AUTORUN';

comment on column XPAT_RUN_SCENARIO.INPUT_DIRECTORY is 'This is OUTPUT directory, where the scenario/transactions paths will be created, and output screenshots and vouchers will be written during the scenario execution runtime.';

comment on column XPAT_RUN_SCENARIO.STATUS is 'The scenario status. Status values/transitions from value to value are not implemented yet, Currently status is always 0. Howewer the fiels is used as transient view to reflect if scenario''s content can be modified, or if current content of scenario is';

comment on column XPAT_RUN_SCENARIO.RUN_COUNT is 'Indicates how many times this scenario was executed';

comment on column XPAT_RUN_SCENARIO.EXECUTABLE is 'Parameters used for scenario grouping,filtering etc.';

comment on column XPAT_RUN_SCENARIO.PARAMETER6 is 'In case if scenario is designed for personal use, it is a name of owner';

