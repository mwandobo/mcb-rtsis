create table TMP_DEP_ACC_GL
(
    ACCOUNT_NUMBER            VARCHAR(40),
    ACCOUNT_CD                SMALLINT,
    DEP_OPEN_UNIT             INTEGER,
    TEKE_EUR_BOOKBAL          DECIMAL(15, 2),
    TEKE_EUR_ACCR_CR_INTEREST DECIMAL(15, 2),
    TEKE_EUR_ACCR_DB_INTEREST DECIMAL(15, 2),
    EOM_BOOK_BALANCE          DECIMAL(15, 2),
    EOM_EUR_ACCR_CR           DECIMAL(15, 2),
    EOM_EUR_ACCR_DB           DECIMAL(15, 2),
    T_E_BOOK_BAL_DIFF         DECIMAL(15, 2),
    T_E_ACCR_CR_DIFF          DECIMAL(15, 2),
    T_E_ACCR_DB_DIFF          DECIMAL(15, 2),
    CR_CNTR_GL_ACC            VARCHAR(21),
    CR_INT_ACCR_GL_ACC        VARCHAR(21),
    DR_INT_ACCR_GL_ACC        VARCHAR(21),
    TD_GL_ACCOUNT             VARCHAR(21),
    TD_CR_INT_ACCR_GL_ACC     VARCHAR(21),
    FIXING                    DECIMAL(8, 4),
    ID_CURRENCY               INTEGER
);

