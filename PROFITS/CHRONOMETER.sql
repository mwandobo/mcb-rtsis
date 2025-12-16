create table CHRONOMETER
(
    EVENT            CHAR(40) not null,
    PROGRAM_ID       CHAR(5)  not null,
    NO_OF_EXECUTIONS DECIMAL(13),
    ELAPSED_TIME     DECIMAL(15),
    constraint IXU_DEF_142
        primary key (EVENT, PROGRAM_ID)
);

