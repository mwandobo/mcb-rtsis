create table GLG_ERROR_LOG
(
    PROCESS_ID CHAR(6) not null,
    START_TIME DATE    not null,
    END_TIME   DATE,
    STATUS     CHAR(1),
    MESSAGE    CHAR(220),
    constraint IXU_GL_002
        primary key (PROCESS_ID, START_TIME)
);

