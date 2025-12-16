create table PAT_EXECUTION_LOOP_ARG_LOG
(
    NAME                       CHAR(32),
    STATIC_DATA                CHAR(100),
    INDEX1                     SMALLINT    not null,
    FK_PAT_LOG_TS_EXECUTION_ID DECIMAL(10) not null
        constraint PATRLVPK
            primary key
);

