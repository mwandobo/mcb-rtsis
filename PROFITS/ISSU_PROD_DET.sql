create table ISSU_PROD_DET
(
    ID        INTEGER,
    PRODUCT   INTEGER,
    MANDATORY CHAR(1)
);

create unique index IXU_ISS_004
    on ISSU_PROD_DET (ID, PRODUCT);

