create table ASSET_DEVELOP_LAWS
(
    LAW_DEVELOPE_CODE       VARCHAR(4) not null
        constraint IXU_GL_011
            primary key,
    YEARS_TOKEEP            SMALLINT,
    TRX_LUNIT               INTEGER,
    TRX_UNIT                INTEGER,
    ADD_DEPREC_YEAR_PERCENT INTEGER,
    TAX_PERCENT             INTEGER,
    TRX_LDATE               DATE,
    VALID_DATE_FROM         DATE,
    TRX_DATE                DATE,
    VALID_DATE_TO           DATE,
    TRX_USR                 CHAR(8),
    TRX_LUSR                CHAR(8),
    CONTROL_OBSERVE         VARCHAR(1),
    ADD_DEPREC_CALC         VARCHAR(2),
    ADD_DEPREC_METHOD       VARCHAR(2),
    SHORT_DECRIPTION        VARCHAR(40),
    DESCRIPTION             VARCHAR(100),
    ADDITIONAL_INFO         VARCHAR(3000)
);

