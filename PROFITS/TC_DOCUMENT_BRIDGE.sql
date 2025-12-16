create table TC_DOCUMENT_BRIDGE
(
    TC_CODE           DECIMAL(10) not null
        constraint IXU_FX_030
            primary key,
    AVAIL_DAYS        SMALLINT,
    VALUE_DAYS        SMALLINT,
    CR_JUSTIF_CODE    INTEGER,
    DR_JUSTIF_CODE    INTEGER,
    GL_CR_JUSTIF_CODE INTEGER,
    GL_DR_JUSTIF_CODE INTEGER,
    JUSTIF_CODE       INTEGER,
    DR_TRX_CODE       INTEGER,
    CR_TRX_CODE       INTEGER,
    TRX_CODE          INTEGER,
    PROD_CODE         INTEGER,
    CHANNEL_ID        INTEGER,
    CREATION_DATE     DATE,
    TMSTAMP           TIMESTAMP(6),
    OPERATION         CHAR(1),
    ENTRY_STATUS      CHAR(1),
    PTJ_OVERCOME      CHAR(1),
    EXTERNAL_SYSTEM   CHAR(20),
    COMMENTS          CHAR(40),
    DESCRIPTION       CHAR(40)
);

