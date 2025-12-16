create table MG_PROGRAM_LOG
(
    START_DATE    TIMESTAMP(6) not null,
    PROGRAM_ID    CHAR(5)      not null,
    CURR_TRX_DATE DATE,
    END_DATE      TIMESTAMP(6),
    BATCH_USER    CHAR(8),
    PROGRAM_DESC  CHAR(40),
    ERR_DESC      CHAR(80),
    constraint IXU_MIG_029
        primary key (START_DATE, PROGRAM_ID)
);

