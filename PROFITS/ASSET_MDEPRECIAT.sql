create table ASSET_MDEPRECIAT
(
    EXTRA_DEPREC_CODE         VARCHAR(4) not null
        constraint IXU_GL_014
            primary key,
    TRX_UNIT                  INTEGER,
    TRX_LUNIT                 INTEGER,
    EXTRA_DEPREC_PERCENT      INTEGER,
    VALID_DATE_TO             DATE,
    TRX_LDATE                 DATE,
    TRX_DATE                  DATE,
    VALID_DATE_FROM           DATE,
    TRX_USR                   CHAR(8),
    TRX_LUSR                  CHAR(8),
    EXTRA_PERC_OF_NORM_DEPREC VARCHAR(1),
    LAW_CODE_SUBJECTED        VARCHAR(4),
    SHORT_DESCRIPTION         VARCHAR(40),
    DESCRIPTION               VARCHAR(100),
    ADDITIONAL_INFO           VARCHAR(3000)
);

