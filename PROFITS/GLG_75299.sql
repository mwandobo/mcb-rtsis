create table GLG_75299
(
    AM             DECIMAL(18) not null
        constraint IXU_REP_029
            primary key,
    UNIT           INTEGER,
    GLG_YPOL_CR    DECIMAL(18, 2),
    GLG_YPOL_DB    DECIMAL(18, 2),
    GLG_YPOL       DECIMAL(18, 2),
    GLG_INVENT     DECIMAL(18, 2),
    GLG_REM_CR     DECIMAL(18, 2),
    GLG_REM_DB     DECIMAL(18, 2),
    GLG_CURRENT_CR DECIMAL(18, 2),
    GLG_CURRENT_DB DECIMAL(18, 2),
    GLG_ACOUNT_ID  CHAR(21)
);

