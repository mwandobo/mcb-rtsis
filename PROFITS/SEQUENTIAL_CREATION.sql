create table SEQUENTIAL_CREATION
(
    TMSTAMP_KEY           TIMESTAMP(6) not null,
    STEP_EXECUTING        DECIMAL(10)  not null,
    SEQUENTIAL_FILE       CHAR(40)     not null,
    SEQUENTIAL_TABLES     CHAR(200),
    SEQUENTIAL_DESCR      CHAR(200),
    SEQUENTIALS_COMPLETED CHAR(50),
    CREATION_TMSTAMP      TIMESTAMP(6),
    TRX_DATE              DATE,
    TRX_UNIT              INTEGER,
    TRX_USER              CHAR(8),
    TRX_CHANNEL           INTEGER,
    TRX_TERMINAL          CHAR(99),
    constraint IXU_R_S_001
        primary key (SEQUENTIAL_FILE, STEP_EXECUTING, TMSTAMP_KEY)
);

