create table POS_ANALYSIS_TMP
(
    ID              INTEGER,
    LOGGED_USER     CHAR(8),
    TUN_INTERNAL_SN SMALLINT,
    ID_JUSTIFIC     INTEGER,
    TRX_CODE        INTEGER,
    TRX_UNIT        INTEGER,
    PRODUCT         INTEGER,
    GL_ARTICLE_CODE INTEGER,
    TRX_SN          INTEGER,
    AMOUNT          DECIMAL(18, 2),
    TRX_DATE        DATE,
    TRX_TYPE        CHAR(2),
    TRX_USER        CHAR(8),
    JUSTIFIC_DESC   VARCHAR(40)
);

create unique index IXU_POS_002
    on POS_ANALYSIS_TMP (ID, LOGGED_USER);

