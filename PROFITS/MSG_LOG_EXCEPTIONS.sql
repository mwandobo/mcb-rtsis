create table MSG_LOG_EXCEPTIONS
(
    ID           DECIMAL(12)  not null
        constraint IXM_LEX_001
            primary key,
    TRIG_PROC    VARCHAR(50)  not null,
    EXCEPTION_NO INTEGER      not null,
    TIME_STAMP   TIMESTAMP(6) not null,
    LOG_MESSAGE  VARCHAR(2000)
);

create unique index IXM_LEX_002
    on MSG_LOG_EXCEPTIONS (TRIG_PROC);

create unique index IXM_LEX_003
    on MSG_LOG_EXCEPTIONS (ID desc, TRIG_PROC asc, TIME_STAMP asc);

