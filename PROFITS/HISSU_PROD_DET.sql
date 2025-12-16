create table HISSU_PROD_DET
(
    ID            INTEGER,
    VALIDITY_DATE DATE,
    PRODUCT       INTEGER,
    MANDATORY     CHAR(1)
);

create unique index IXU_HIS_021
    on HISSU_PROD_DET (ID, VALIDITY_DATE, PRODUCT);

