create table BOP_FXFT_CRITER_T
(
    TRX_CODE        INTEGER,
    NON_RESIDENT    CHAR(1),
    LCY_FCY         CHAR(3),
    REL_TRX_CAT     CHAR(5),
    TRX_CATEGORY    CHAR(5),
    DB_CODE         CHAR(10),
    OTHER_COLL_BANK CHAR(10),
    REL_BOPFC       CHAR(10),
    REL_BOPEC       CHAR(10),
    REL_CR_CODE     CHAR(10),
    BOPFC           CHAR(10),
    CR_CODE         CHAR(10),
    REL_DB_CODE     CHAR(10),
    BOPEC           CHAR(10)
);

create unique index PK_BOPCRBD_T
    on BOP_FXFT_CRITER_T (NON_RESIDENT, LCY_FCY, TRX_CATEGORY, TRX_CODE);

