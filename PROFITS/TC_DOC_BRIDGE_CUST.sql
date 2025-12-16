create table TC_DOC_BRIDGE_CUST
(
    TC_CODE           DECIMAL(10) not null,
    CUST_ID           INTEGER     not null,
    AVAIL_DAYS        SMALLINT,
    VALUE_DAYS        SMALLINT,
    CR_TRX_CODE       INTEGER,
    DR_TRX_CODE       INTEGER,
    TRX_CODE          INTEGER,
    PROD_CODE         INTEGER,
    JUSTIF_CODE       INTEGER,
    GL_DR_JUSTIF_CODE INTEGER,
    CR_JUSTIF_CODE    INTEGER,
    DR_JUSTIF_CODE    INTEGER,
    GL_CR_JUSTIF_CODE INTEGER,
    constraint IXU_FX_029
        primary key (TC_CODE, CUST_ID)
);

