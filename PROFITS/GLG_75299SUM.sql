create table GLG_75299SUM
(
    LEVEL0         CHAR(1) not null,
    UNITCODE       INTEGER not null,
    GLG_REM_DB     DECIMAL(18, 2),
    GLG_CURRENT_DB DECIMAL(18, 2),
    GLG_CURRENT_CR DECIMAL(18, 2),
    GLG_YPOL       DECIMAL(18, 2),
    GLG_REM_CR     DECIMAL(18, 2),
    GLG_INVENT     DECIMAL(18, 2),
    GLG_YPOL_DB    DECIMAL(18, 2),
    GLG_YPOL_CR    DECIMAL(18, 2),
    LEVEL0_DESC    CHAR(40),
    constraint IXU_REP_052
        primary key (LEVEL0, UNITCODE)
);

