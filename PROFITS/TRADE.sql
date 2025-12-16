create table TRADE
(
    FK_PRDID_PRODUCT   INTEGER not null
        constraint IXU_FX_037
            primary key,
    FKGD_HAS_AS_CRLINE INTEGER,
    FK_GEN_DET_S_NUM   INTEGER,
    TMSTAMP            TIMESTAMP(6),
    TRADE_TYPE         CHAR(1),
    FK_GEN_DET_HEAD    CHAR(5),
    FKGH_HAS_AS_CRLINE CHAR(5),
    TRADE_FLAGS        CHAR(20),
    CR_GL_ACC          CHAR(21),
    NET_GL_ACC         CHAR(21),
    DB_GL_ACC          CHAR(21),
    COMM_CHRG_FRQ      SMALLINT,
    COMM_CHRG_FRQT     CHAR(1)
);

create unique index I0001036
    on TRADE (FKGD_HAS_AS_CRLINE, FKGH_HAS_AS_CRLINE);

create unique index I00010430
    on TRADE (FK_GEN_DET_S_NUM, FK_GEN_DET_HEAD);

