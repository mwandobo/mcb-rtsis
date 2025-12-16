create table ASSET_HCATEGORIES
(
    CATEGORY_CODE      VARCHAR(2) not null
        constraint IXU_GL_051
            primary key,
    TRX_UNIT           INTEGER,
    TRX_LUNIT          INTEGER,
    TRX_DATE           DATE,
    TRX_LDATE          DATE,
    TRX_USR            CHAR(8),
    TRX_LUSR           CHAR(8),
    OPPOSED_GL_ACCOUNT CHAR(21),
    GL_ACCOUNT         CHAR(21),
    DEPRECIAT_GL_ACCNT CHAR(21),
    DEPRICIAT_MTD_EXEC VARCHAR(2),
    DEPRICIAT_MTD      VARCHAR(2),
    LAW_CODE_SUBJECTED VARCHAR(4),
    DESCRIPTION        VARCHAR(100),
    VATABLE            DECIMAL(1) default 1,
    REGISTRY_TYPE      CHAR(2)
);

