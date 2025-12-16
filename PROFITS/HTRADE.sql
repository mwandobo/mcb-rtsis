create table HTRADE
(
    FK_HPRDID_PRODUCT  INTEGER not null,
    FK_HPRDVAL_DATE    DATE    not null,
    FKGD_HAS_AS_CRLINE INTEGER,
    FK_GEN_DET_S_NUM   INTEGER,
    TMSTAMP            TIMESTAMP(6),
    TRADE_TYPE         CHAR(1),
    FKGH_HAS_AS_CRLINE CHAR(5),
    FK_GEN_DET_HEAD    CHAR(5),
    TRADE_FLAGS        CHAR(20),
    NET_GL_ACC         CHAR(21),
    DB_GL_ACC          CHAR(21),
    CR_GL_ACC          CHAR(21),
    COMM_CHRG_FRQ      SMALLINT,
    COMM_CHRG_FRQT     CHAR(1),
    constraint IXU_PRD_002
        primary key (FK_HPRDID_PRODUCT, FK_HPRDVAL_DATE)
);

