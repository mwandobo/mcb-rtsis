create table DCD_EXIT_STATE_TMP
(
    EXIT_STATE_DESC    CHAR(40)    not null,
    NUM_OCCUR          DECIMAL(15) not null,
    PRFT_SYSTEM        SMALLINT,
    LANGUAGE_USED      INTEGER,
    MODEL_ID           DECIMAL(12),
    ID                 DECIMAL(12),
    FROM_ENCYCLOPEDIA  CHAR(1),
    TERMINATION_ACTION CHAR(2),
    MESSAGE_TYPE       CHAR(2),
    ACTUAL_MESSAGE     VARCHAR(2048),
    constraint IXU_DEF_100
        primary key (EXIT_STATE_DESC, NUM_OCCUR)
);

