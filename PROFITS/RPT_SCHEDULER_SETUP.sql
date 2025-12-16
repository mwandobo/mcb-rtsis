create table RPT_SCHEDULER_SETUP
(
    ID                 INTEGER            not null
        constraint RPT_SCHEDULER_SETUP_PK
            primary key,
    FK_REPORT_ID       INTEGER            not null,
    STATUS             SMALLINT default 0 not null,
    STARTING_EXECUTION TIME               not null,
    ENDING_EXECUTION   TIME               not null,
    EXECUTION_STEP     TIME               not null,
    NEXT_EXECUTION     TIMESTAMP(6),
    LAST_EXECUTION     TIMESTAMP(6),
    PER_DAY_FREQUENCY  SMALLINT,
    WEEK_BASED         SMALLINT,
    MONTH_BASED        BIGINT,
    CREATED            TIMESTAMP(6)       not null,
    UPDATED            TIMESTAMP(6),
    CREATED_BY         VARCHAR(50)        not null,
    UPDATED_BY         VARCHAR(50),
    FK_LANGUAGE_ID     INTEGER,
    PROTECT            SMALLINT default 1,
    DESCRIPTION        VARCHAR(4000),
    FK_DATABASE_ID     INTEGER  default 0 not null,
    FREE_TEXT          VARCHAR(200)
);

