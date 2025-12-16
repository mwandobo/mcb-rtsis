create table GLG_INTFC_IMP_DTL
(
    FK_GLG_INTFC_IMTRX DATE     not null,
    FK_GLG_INTFC_IMSUB CHAR(2)  not null,
    FK_GLG_INTFC_IMFIL SMALLINT not null,
    REC_SN             INTEGER  not null,
    ACC_UNIT           INTEGER,
    FREQUENCY          INTEGER,
    TRX_UNIT           INTEGER,
    CUST_CODE          INTEGER,
    TRX_USR_SN         INTEGER,
    TRX_RATE           DECIMAL(12, 6),
    DET_AM6            DECIMAL(15, 2),
    DET_AM4            DECIMAL(15, 2),
    DET_AM3            DECIMAL(15, 2),
    DET_AM2            DECIMAL(15, 2),
    DET_AM1            DECIMAL(15, 2),
    DET_AM5            DECIMAL(15, 2),
    TIMESTMP           TIMESTAMP(6),
    TRNSF_DATE         DATE,
    TRX_DATE           DATE,
    VALUE_DATE         DATE,
    TRNSF_FLG          CHAR(1),
    TRG_CURR_SDESC     CHAR(5),
    SRC_CURR_SDESC     CHAR(5),
    TRX_CODE           CHAR(5),
    TRX_USR            CHAR(8),
    MOVEMENT_CODE      CHAR(20),
    CUST_ACC_CODE      CHAR(40),
    ERROR_DESC         VARCHAR(100),
    constraint IXU_GL_037
        primary key (FK_GLG_INTFC_IMTRX, FK_GLG_INTFC_IMSUB, FK_GLG_INTFC_IMFIL, REC_SN)
);

