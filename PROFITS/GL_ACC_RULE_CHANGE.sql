create table GL_ACC_RULE_CHANGE
(
    TIMESTAMP       TIMESTAMP(6) not null
        constraint IXU_DEF_084
            primary key,
    ID_PRODUCT      INTEGER,
    OLD_GL_ACC_RULE INTEGER,
    ID_JUSTIFIC     INTEGER,
    TRX_UNIT        INTEGER,
    NEW_GL_ACC_RULE INTEGER,
    TRX_CODE        INTEGER,
    TRX_DATE        DATE,
    TRX_USR         CHAR(8)
);

