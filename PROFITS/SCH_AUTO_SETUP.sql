create table SCH_AUTO_SETUP
(
    ID                          VARCHAR(40)           not null
        constraint SCH_AUTO_SETUP_PK
            primary key,
    FK_SCRIPT_ID                VARCHAR(40)           not null,
    FK_SCH_SCRIPT_RUN_PARAM     VARCHAR(40)           not null,
    ACTIVE                      SMALLINT    default 0 not null,
    LAST_USER                   VARCHAR(20),
    LAST_UPDATED                TIMESTAMP(6),
    STARTING_EXECUTION          TIME                  not null,
    ENDING_EXECUTION            TIME                  not null,
    EXECUTION_STEP              TIME                  not null,
    NEXT_EXECUTION              TIMESTAMP(6),
    LAST_EXECUTION              TIMESTAMP(6),
    PRIORITY                    SMALLINT    default 0 not null,
    PER_DAY_FREQUENCY           SMALLINT    default 0 not null,
    WEEK_BASED                  SMALLINT,
    MONTH_BASED                 INTEGER,
    STOP_ON_ERROR               SMALLINT    default 0 not null,
    PROCEED_VAL_ERROR           SMALLINT    default 0 not null,
    CONCURRENT_EXECUTION        SMALLINT    default 0 not null,
    FK_SCH_SCRIPT_RUN_SEC_PARAM VARCHAR(40) default 'NULL'
);

