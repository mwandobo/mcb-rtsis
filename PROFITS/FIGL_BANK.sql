create table FIGL_BANK
(
    RWCUR          VARCHAR(5),
    GL_ACCOUNT     VARCHAR(9),
    PSTNG_DATE     DATE,
    DATEGLCURRKEY  VARCHAR(30) not null,
    DEB_CRE_LC     DECIMAL(18, 2),
    CUM_DEB_CRE_LC DECIMAL(18, 2)
);

create unique index IX_S1_FIBANK
    on FIGL_BANK (PSTNG_DATE, GL_ACCOUNT);

