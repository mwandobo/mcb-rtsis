create table ASSET_DCATEGORIES
(
    CATEGORY_CODE      VARCHAR(2) not null,
    ROW_NUMBER         INTEGER    not null,
    TRX_UNIT           INTEGER,
    TRX_LUNIT          INTEGER,
    ANNUAL_DEPR_PERCNT DECIMAL(8, 4),
    TRX_DATE           DATE,
    TRX_LDATE          DATE,
    VALID_TO           DATE,
    VALID_FROM         DATE,
    TRX_LUSR           CHAR(8),
    TRX_USR            CHAR(8),
    constraint IXU_GL_009
        primary key (CATEGORY_CODE, ROW_NUMBER)
);

