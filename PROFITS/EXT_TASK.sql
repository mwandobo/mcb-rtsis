create table EXT_TASK
(
    EXT_TASK_ID          DECIMAL(10) not null
        constraint PK_EXTTSK1
            primary key,
    EXT_TASK_DESCRIPTION CHAR(80),
    EXT_TASK_SYSTEM      SMALLINT,
    SEND_EMAIL           CHAR(1),
    SEND_SMS             CHAR(1),
    EXT_TASK_ERROR       CHAR(40),
    PROGRAM_TMSTAMP      TIMESTAMP(6),
    START_TMSTAMP        TIMESTAMP(6),
    END_TMSTAMP          TIMESTAMP(6),
    EXT_TASK_SQL         VARCHAR(4000),
    EXT_QUESTION_SQL     VARCHAR(4000),
    EXT_MESSAGE          VARCHAR(4000),
    EXT_TEXT_RETURN      CHAR(1)
);

