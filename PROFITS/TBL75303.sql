create table TBL75303
(
    ID            INTEGER not null
        constraint IXU_REP_172
            primary key,
    REC_LEVEL     SMALLINT,
    YPOL_DB       DECIMAL(18, 2),
    CURRENT_DB    DECIMAL(18, 2),
    CURRENT_CR    DECIMAL(18, 2),
    YPOL          DECIMAL(18, 2),
    YPOL_CR       DECIMAL(18, 2),
    INVENTORY     DECIMAL(18, 2),
    GLG_INVENT    DECIMAL(18, 2),
    GLG_REM_CR    DECIMAL(18, 2),
    GLG_REM_DB    DECIMAL(18, 2),
    CUR_TIMESTAMP TIMESTAMP(6),
    ACCOUNT_ID    CHAR(21),
    REC_DESCR     CHAR(40),
    ACCOUNT_DESCR CHAR(60)
);

