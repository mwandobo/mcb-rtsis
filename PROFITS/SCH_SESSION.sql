create table SCH_SESSION
(
    ID                          TIMESTAMP(6) not null
        constraint SCH_SESSION_PK
            primary key,
    RUNNED_BY_USER              VARCHAR(20)  not null,
    FK_SCRIPT                   VARCHAR(40)  not null,
    FK_RUN_PARAMETERS           VARCHAR(40)  not null,
    STARTED                     TIMESTAMP(6) not null,
    ENDED                       TIMESTAMP(6),
    RUNNED                      TIME,
    HOSTNAME                    VARCHAR(63) default 'NA',
    IPADDRESS                   VARCHAR(46) default '0.0.0.0',
    BEFORE_SCHEDULED_DATE       TIMESTAMP(0),
    BEFORE_PREV_TRX_DATE        TIMESTAMP(0),
    BEFORE_NEXT_TRX_DATE        TIMESTAMP(0),
    BEFORE_CURR_TRX_DATE        TIMESTAMP(0),
    BEFORE_PREVIOUS_DATE        TIMESTAMP(0),
    AFTER_SCHEDULED_DATE        TIMESTAMP(0),
    AFTER_PREV_TRX_DATE         TIMESTAMP(0),
    AFTER_NEXT_TRX_DATE         TIMESTAMP(0),
    AFTER_CURR_TRX_DATE         TIMESTAMP(0),
    AFTER_PREVIOUS_DATE         TIMESTAMP(0),
    LASTPING                    TIMESTAMP(6),
    FK_RUN_SECONDARY_PARAMETERS VARCHAR(40) default 'NULL'
);

