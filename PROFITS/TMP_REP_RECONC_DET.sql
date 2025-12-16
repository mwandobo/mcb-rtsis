create table TMP_REP_RECONC_DET
(
    PROF_ACCOUNT      CHAR(40),
    CHECK_DIGIT       SMALLINT,
    ORIGIN_TYPE       CHAR(1),
    ORIGIN_ID         CHAR(2),
    CHARGE_CODE       INTEGER,
    END_SYS_BALANCE   DECIMAL(18, 2),
    START_SYS_BALANCE DECIMAL(18, 2),
    SYS_DIF           DECIMAL(18, 2),
    DR_GL             DECIMAL(18, 2),
    CR_GL             DECIMAL(18, 2),
    GL_DIF            DECIMAL(18, 2),
    DIF               DECIMAL(18, 2),
    CON_DR_GL         DECIMAL(18, 2),
    CON_CR_GL         DECIMAL(18, 2)
);

