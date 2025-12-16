create table BOP_FXFT_CRITER_C
(
    CORR_CUST_ID    INTEGER,
    NON_RESIDENT    CHAR(1),
    LCY_FCY         CHAR(3),
    TRX_CATEGORY    CHAR(5),
    DB_CODE         CHAR(10),
    CR_CODE         CHAR(10),
    BOPFC           CHAR(10),
    OTHER_COLL_BANK CHAR(10),
    BOPEC           CHAR(10)
);

create unique index PK_BOPCRBD_C
    on BOP_FXFT_CRITER_C (NON_RESIDENT, LCY_FCY, TRX_CATEGORY, CORR_CUST_ID);

