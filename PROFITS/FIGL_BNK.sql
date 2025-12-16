create table FIGL_BNK
(
    DATEGLKEY      DECIMAL(19) not null,
    GL_ACCOUNT     VARCHAR(9),
    PSTNG_DATE     DATE,
    DEB_CRE_LC     DECIMAL(18, 2),
    CUM_DEB_CRE_LC DECIMAL(18, 2),
    DEB_CRE_FX     DECIMAL(18, 2),
    CUM_DEB_CRE_FX DECIMAL(18, 2)
);

create unique index INDX_FIGL_BNK
    on FIGL_BNK (PSTNG_DATE, GL_ACCOUNT);

