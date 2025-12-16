create table GLG_PROCESS_LOG
(
    PROCESS_ID      CHAR(6)      not null,
    START_TIME      TIMESTAMP(6) not null,
    END_TIME        TIMESTAMP(6),
    PROCESS_MESSAGE CHAR(220),
    PROCESS_STATUS  CHAR(1),
    constraint I0000724
        primary key (PROCESS_ID, START_TIME)
);

