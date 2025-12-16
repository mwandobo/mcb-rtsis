create table TEMP_GLI_EXTR_DAY
(
    TUN_INTERNAL_SN    SMALLINT,
    FK_UNITCODETRXUNIT INTEGER,
    TRX_SN             INTEGER,
    TRX_GL_TRN_DATE    DATE,
    TRN_DATE           DATE,
    FK_USRCODE         CHAR(8),
    TRX_USR            CHAR(8)
);

