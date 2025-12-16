create table ASYN_AUTHORIZ_LOG
(
    TIMESTAMP         TIMESTAMP(6),
    TRX_USER          CHAR(8),
    TRX_UNIT          INTEGER,
    TRX_DATE          DATE,
    TRX_CODE          INTEGER,
    RECORD_STATUS     CHAR(1),
    FK_UNIT_CATEGORID CHAR(8),
    MAC_ADDRESS       CHAR(30),
    COMMENTS          CHAR(40),
    PRIMARY_KEY       CHAR(254)
);

create unique index IXU_ASY_001
    on ASYN_AUTHORIZ_LOG (TIMESTAMP, TRX_USER, TRX_UNIT, TRX_DATE);

