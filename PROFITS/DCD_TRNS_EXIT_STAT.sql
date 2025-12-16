create table DCD_TRNS_EXIT_STAT
(
    CODE               CHAR(8)      not null,
    ID                 DECIMAL(12)  not null,
    LANGUAGE_USED      INTEGER      not null,
    PRFT_SYSTEM        SMALLINT     not null,
    TMPSTAMP           TIMESTAMP(6) not null,
    MODEL_ID           DECIMAL(12),
    FROM_ENCYCLOPEDIA  CHAR(1),
    STATUS0            CHAR(1),
    MESSAGE_TYPE       CHAR(2),
    TERMINATION_ACTION CHAR(2),
    PASSWORD           CHAR(26),
    EXIT_STATE_DESC    CHAR(40),
    ACTUAL_MESSAGE     VARCHAR(2048),
    constraint IXU_DEF_063
        primary key (CODE, ID, LANGUAGE_USED, PRFT_SYSTEM, TMPSTAMP)
);

