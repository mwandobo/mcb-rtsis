create table CLC_INT_MAX_TIMESTAMPS
(
    PROC_NAME   VARCHAR(80) not null,
    TABLE_NAME  VARCHAR(80) not null,
    MAX_TMSTAMP TIMESTAMP(6),
    constraint PK_CLC_INT_MAX_TIM
        primary key (TABLE_NAME, PROC_NAME)
);

