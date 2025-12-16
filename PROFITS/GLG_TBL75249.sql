create table GLG_TBL75249
(
    FK_GLG_HD_TBL75HD INTEGER not null,
    ID                INTEGER not null,
    CODE              INTEGER,
    ID_CURRENCY       INTEGER,
    RATE              DECIMAL(12, 6),
    PROGRESSIVE_CR    DECIMAL(15, 2),
    AMOUNT_DB         DECIMAL(15, 2),
    AMOUNT_CR         DECIMAL(15, 2),
    CURR_BAL          DECIMAL(15, 2),
    PREV_BAL          DECIMAL(15, 2),
    PROGRESSIVE_DB    DECIMAL(15, 2),
    TRN_FIX_BAL       DECIMAL(15, 2),
    ACTIVATION_DATE   DATE,
    CUR_TIMSTAMP      TIMESTAMP(6),
    ACCOUNT_ID        CHAR(21),
    UNIT_NAME         CHAR(40),
    DESCRIPTION       CHAR(40),
    DESCR             CHAR(60),
    constraint IXU_GL_041
        primary key (FK_GLG_HD_TBL75HD, ID)
);

