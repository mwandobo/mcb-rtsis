create table DCD_ROUTINE_TMP
(
    NUM_OCCUR          DECIMAL(15) not null,
    ROUTINE_NAME       CHAR(80)    not null,
    PRFT_SYSTEM        SMALLINT,
    ROUTINE_SN         DECIMAL(12),
    MODEL_ID           DECIMAL(12),
    SOURCE_NAME        CHAR(10),
    FUNCTIONALITY_DESC VARCHAR(2048),
    constraint IXU_DEF_057
        primary key (NUM_OCCUR, ROUTINE_NAME)
);

