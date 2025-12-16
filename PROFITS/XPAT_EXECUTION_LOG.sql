create table XPAT_EXECUTION_LOG
(
    TS_EXECUTION_ID           DECIMAL(10) not null
        constraint PATXEXPK
            primary key,
    INDEX1                    INTEGER     not null,
    EXECUTION_START_TIMESTAMP TIMESTAMP(6),
    STATUS                    CHAR(3)     not null,
    RUN_COUNT                 INTEGER     not null,
    EXECUTION_END_TIMESTAMP   TIMESTAMP(6),
    LAST_INSTRUCTION_SEQ_ID   DECIMAL(10) not null,
    FUTURE1                   CHAR(10),
    FUTURE2                   CHAR(10),
    FUTURE3                   CHAR(240),
    FUTURE4                   CHAR(60),
    FUTURE5                   CHAR(1),
    FUTURE6                   CHAR(2),
    DISABLED                  CHAR(1),
    FK_XPAT_RUN_SCEID         INTEGER,
    FK_XPAT_TRANSACID         INTEGER
);

comment on column XPAT_EXECUTION_LOG.INDEX1 is 'Defines order of the scenarios execution';

comment on column XPAT_EXECUTION_LOG.STATUS is 'Spaces: defined, but not started yet,*** defined, but commented out, will be omitted during execution.BEG: just started,OK finished okT_V terminated due expected value mismatchT_T terminated due to timeout T_E terminated due error occured CNL Cancelled';

comment on column XPAT_EXECUTION_LOG.RUN_COUNT is 'Holds the value of the Run_Count attribute of the Scenario. It is used for distinguishing of successive executions of same scenario.';

comment on column XPAT_EXECUTION_LOG.FUTURE1 is 'Reserved for future use, (initially was PROFILE_ID)';

comment on column XPAT_EXECUTION_LOG.FUTURE2 is 'Reserved for future use, (initially was BANK_NAME)';

comment on column XPAT_EXECUTION_LOG.FUTURE3 is 'Reserved for future use, (initially was PROFITS DB connect string, This field is used alsp for chef profile look-up.';

comment on column XPAT_EXECUTION_LOG.FUTURE4 is 'Reserved for future use, (initially was TESTER_NAME)';

comment on column XPAT_EXECUTION_LOG.FUTURE5 is 'Reserved for future use, (initially was CLIENT_TYPE)';

comment on column XPAT_EXECUTION_LOG.FUTURE6 is 'Reserved for future use, (initially was LANGUAGE_CODE)';

create unique index PATXEXI1
    on XPAT_EXECUTION_LOG (FK_XPAT_RUN_SCEID);

create unique index PATXEXI2
    on XPAT_EXECUTION_LOG (FK_XPAT_TRANSACID);

