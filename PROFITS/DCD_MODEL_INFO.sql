create table DCD_MODEL_INFO
(
    MODEL_ID    DECIMAL(12) not null
        constraint IXU_DEF_007
            primary key,
    PRFT_SYSTEM SMALLINT,
    MODEL_NAME  CHAR(80)
);

