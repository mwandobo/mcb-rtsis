create table FIGL_O02_CCY
(
    DATEGLKEY      DECIMAL(19) not null,
    GL_ACCOUNT     VARCHAR(9),
    PSTNG_DATE     DATE,
    DEB_CRE_LC     DECIMAL(18, 2),
    CUM_DEB_CRE_LC DECIMAL(18, 2),
    CURRENCY       VARCHAR(20)
);

