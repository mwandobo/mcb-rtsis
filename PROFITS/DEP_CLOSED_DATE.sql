create table DEP_CLOSED_DATE
(
    TRX_DATE          DATE not null
        constraint IXU_DEP_121
            primary key,
    TIMESTMP          DATE,
    ENTRY_STATUS      CHAR(1),
    BATCH_PROCESS_FLG CHAR(1)
);

