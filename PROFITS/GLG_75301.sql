create table GLG_75301
(
    AM             DECIMAL(18) not null
        constraint IXU_REP_030
            primary key,
    GLG_CURRENT_DB DECIMAL(18, 2),
    GLG_CURRENT_CR DECIMAL(18, 2),
    GLG_REM_DB     DECIMAL(18, 2),
    GLG_YPOL       DECIMAL(18, 2),
    GLG_INVENT     DECIMAL(18, 2),
    GLG_YPOL_CR    DECIMAL(18, 2),
    GLG_REM_CR     DECIMAL(18, 2),
    GLG_YPOL_DB    DECIMAL(18, 2),
    GLG_ACCOUNT_ID CHAR(21)
);

