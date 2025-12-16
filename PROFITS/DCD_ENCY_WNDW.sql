create table DCD_ENCY_WNDW
(
    ENCY_MODEL_ID DECIMAL(12) not null,
    ENCY_PROC_ID  DECIMAL(12) not null,
    PRFT_PROC_ID  INTEGER,
    TMPSTAMP      TIMESTAMP(6),
    PROC_NAME     CHAR(40),
    constraint IXU_DEF_099
        primary key (ENCY_MODEL_ID, ENCY_PROC_ID)
);

