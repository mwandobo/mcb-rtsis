create table RPT_SCHEDULER_RESULT
(
    TIME_STAMP          TIMESTAMP(6)  not null,
    FK_REPORT_RESULT_ID BIGINT        not null,
    FK_REPORT_ID        INTEGER       not null,
    FK_SCHEDULER_ID     INTEGER       not null,
    NEXT_EXECUTION      TIMESTAMP(6)  not null,
    SCHEDULED_EXECUTION TIMESTAMP(6)  not null,
    HOST_IP             VARCHAR(20)   not null,
    LOG                 VARCHAR(4000) not null
);

create unique index RPT_SCHEDULER_RESULT_PK
    on RPT_SCHEDULER_RESULT (TIME_STAMP);

