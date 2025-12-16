create table TPP_BATCH_PARAM_LINE
(
    LOT_NO          CHAR(10) not null,
    PROGRAM_ID      CHAR(5)  not null,
    CP_AGREEMENT_NO DECIMAL(10),
    EXEC_DATE       DATE,
    TMSTAMP         TIMESTAMP(6),
    constraint IXU_TPP_003
        primary key (LOT_NO, PROGRAM_ID)
);

