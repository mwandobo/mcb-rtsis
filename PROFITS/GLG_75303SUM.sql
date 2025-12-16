create table GLG_75303SUM
(
    LEVEL0         CHAR(1) not null
        constraint IXU_REP_054
            primary key,
    GLG_REM_CR     DECIMAL(18, 2),
    GLG_CURRENT_DB DECIMAL(18, 2),
    GLG_CURRENT_CR DECIMAL(18, 2),
    GLG_REM_DB     DECIMAL(18, 2),
    INVENTORY      DECIMAL(18, 2),
    GLG_INVERT     DECIMAL(18, 2),
    GLG_YPOL_CR    DECIMAL(18, 2),
    GLG_YPOL_DB    DECIMAL(18, 2),
    GLG_YPOL       DECIMAL(18, 2),
    LEVEL0_DESC    CHAR(40)
);

