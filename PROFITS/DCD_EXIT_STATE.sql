create table DCD_EXIT_STATE
(
    PRFT_SYSTEM        SMALLINT    not null,
    ID                 DECIMAL(12) not null,
    LANGUAGE_USED      INTEGER     not null,
    EXIT_STATE_DESC    CHAR(40)    not null,
    TERMINATION_ACTION CHAR(2),
    MESSAGE_TYPE       CHAR(2),
    FROM_ENCYCLOPEDIA  CHAR(1),
    MODEL_ID           DECIMAL(12),
    ACTUAL_MESSAGE     VARCHAR(2048),
    constraint PKDCD02
        primary key (PRFT_SYSTEM, ID, LANGUAGE_USED)
);

