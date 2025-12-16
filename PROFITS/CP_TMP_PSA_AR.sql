create table CP_TMP_PSA_AR
(
    FK_PARAMETRIC_RID INTEGER,
    TRX_CODE          INTEGER,
    I_ID_JUSTIFIC     INTEGER,
    GL_RULE_CODE      INTEGER,
    ID_PRODUCT        INTEGER,
    TRN_TYPE          CHAR(1),
    JC_FLAG           CHAR(1),
    HAS_390           CHAR(1),
    HAS_380           CHAR(1),
    HAS_54            CHAR(1),
    HAS_55            CHAR(1),
    CHECK_FLAG        CHAR(6),
    PRODUCT_DESC      CHAR(40),
    TRANSAC_DESC      CHAR(40),
    PARAMRUL_DESC     CHAR(40),
    GLRULE_DESC       CHAR(40),
    JUSTIFIC_DESC     CHAR(40)
);

