create table BOP_FXFT_CRITERIA
(
    BASE_ON_NEXTL   SMALLINT,
    CORR_CUST_ID    INTEGER,
    TRX_AMOUNT      DECIMAL(15, 2),
    NON_RESIDENT    CHAR(1),
    LCY_FCY         CHAR(3),
    TRX_CATEGORY    CHAR(5),
    OTHER_COLL_BANK CHAR(10),
    DB_CODE         CHAR(10),
    BOPFC           CHAR(10),
    BOPEC           CHAR(10),
    CR_CODE         CHAR(10)
);

create unique index PK_BOPCRBD
    on BOP_FXFT_CRITERIA (NON_RESIDENT, TRX_CATEGORY, LCY_FCY);

